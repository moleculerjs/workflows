/*
 * @moleculer/channels
 * Copyright (c) 2021 MoleculerJS (https://github.com/moleculerjs/channels)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const BaseAdapter = require("./base");
const { MoleculerError, MoleculerRetryableError } = require("moleculer").Errors;
const C = require("../constants");
const Redis = require("ioredis");
const { parseDuration } = require("../utils");

/**
 * @typedef {import("ioredis").Cluster} Cluster Redis cluster instance. More info: https://github.com/luin/ioredis/blob/master/API.md#Cluster
 * @typedef {import("ioredis").Redis} Redis Redis instance. More info: https://github.com/luin/ioredis/blob/master/API.md#Redis
 * @typedef {import("ioredis").RedisOptions} RedisOptions
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("../index").Channel} Channel Base channel definition
 * @typedef {import("./base").BaseDefaultOptions} BaseDefaultOptions Base adapter options
 */

/**
 * Redis Streams adapter
 *
 * @class RedisAdapter
 * @extends {BaseAdapter}
 */
class RedisAdapter extends BaseAdapter {
	/**
	 * Constructor of adapter.
	 *
	 * @param  {Object?} opts
	 */
	constructor(opts) {
		if (_.isString(opts))
			opts = {
				redis: {
					url: opts
				}
			};

		if (opts && _.isString(opts.redis)) {
			opts = {
				...opts,
				redis: {
					url: opts.redis
				}
			};
		}

		super(opts);

		/** @type {RedisOpts & BaseDefaultOptions} */
		this.opts = _.defaultsDeep(this.opts, {
			drainDelay: 5,
			lockDuration: 30 * 1000,
			redis: {
				retryStrategy: times => Math.min(times * 500, 5000)
			}
		});

		this.jobClients = new Map();
		this.commandClient = null;
		this.signalSubClient = null;

		this.signalPromises = new Map();

		this.connected = false;
		this.disconnecting = false;
		this.isRunning = false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {ServiceBroker} broker
	 * @param {Logger} logger
	 */
	init(broker, logger) {
		super.init(broker, logger);

		if (this.opts.prefix) {
			this.prefix = this.opts.prefix + ":";
		} else if (this.broker.namespace) {
			this.prefix = "MOLWF-" + this.broker.namespace + ":";
		} else {
			this.prefix = "MOLWF:";
		}

		this.logger.info("Workflows Redis adapter prefix:", this.prefix);
	}

	/**
	 * Connect to Redis.
	 */
	async connect() {
		if (this.connected) return;

		await this.createCommandClient();
		await this.createSignalClient();

		await this.afterConnected();
	}

	createCommandClient() {
		return new Promise((resolve, reject) => {
			const client = this.createRedisClient();

			client.on("ready", () => {
				this.commandClient = client;
				this.connected = true;
				resolve(client);
				this.logger.info("Workflows Redis adapter connected.");
			});
			client.on("error", err => {
				this.connected = false;
				this.logger.error("Workflows Redis adapter error", err.message);
			});
			client.on("end", () => {
				this.connected = false;
				this.commandClient = null;
				this.logger.info("Workflows Redis adapter disconnected.");
			});
		});
	}

	createSignalClient() {
		return new Promise((resolve, reject) => {
			const client = this.createRedisClient();

			client.on("ready", () => {
				this.signalSubClient = client;
				this.connected = true;

				client.subscribe(this.getKey(C.QUEUE_SIGNAL));

				resolve(client);
				//this.logger.info("Workflows Redis adapter connected.");
			});
			client.on("error", err => {
				this.connected = false;
				this.logger.error("Workflows Redis adapter error", err.message);
			});
			client.on("end", () => {
				this.connected = false;
				this.signalSubClient = null;
				//this.logger.info("Workflows Redis adapter disconnected.");
			});
			client.on("message", (channel, message) => {
				if (channel === this.getKey(C.QUEUE_SIGNAL)) {
					const json = this.serializer.deserialize(message);
					this.logger.debug("Signal received on Pub/Sub", json);
					const pKey = json.signal + ":" + json.key;
					const found = this.signalPromises.get(pKey);
					if (found) {
						this.signalPromises.delete(pKey);
						found.resolve(json.payload);
					}
				}
			});
		});
	}

	/**
	 * Disconnect from adapter
	 */
	async disconnect() {
		if (this.disconnecting) return;

		this.disconnecting = true;
		this.connected = false;

		if (this.blockedClient) {
			await this.blockedClient.quit();
			this.blockedClient = null;
		}

		if (this.commandClient) {
			await this.commandClient.quit();
			this.commandClient = null;
		}

		if (this.signalSubClient) {
			await this.signalSubClient.quit();
			this.signalSubClient = null;
		}

		this.disconnecting = false;
	}

	/**
	 * Wait for Redis client to be ready. If it's not exists, it will create a new one.
	 *
	 * @param {String} workflowName
	 * @returns {Redis}
	 */
	async isClientReady(workflowName) {
		return new Promise((resolve, reject) => {
			let client = this.jobClients[workflowName];
			if (client && client.status === "ready") {
				resolve(client);
			} else {
				if (!client) {
					client = this.createRedisClient();
					this.jobClients[workflowName] = client;
				}

				const handleReady = () => {
					client.removeListener("end", handleEnd);
					client.removeListener("error", handleError);
					resolve(client);
				};

				let lastError;
				const handleError = err => {
					lastError = err;
				};

				const handleEnd = () => {
					client.removeListener("ready", handleReady);
					client.removeListener("error", handleError);
					reject(lastError);
				};

				client.once("ready", handleReady);
				client.on("error", handleError);
				client.once("end", handleEnd);
			}
		});
	}

	startJobProcessor(workflow) {
		if (!this.jobClients[workflow]) {
			this.runJobProcessor(workflow);
		}
	}

	async stopJobProcessor(workflow) {
		if (this.jobClients[workflow]) {
			await this.jobClients[workflow].quit();
			this.jobClients[workflow].stopped = true;
		}
	}

	/**
	 * Job processor for the given workflow. It will wait for a job in the waiting queue and move it to the active queue.
	 * Then it will call the workflow handler with the job.
	 *
	 * @param {Workflow} workflow
	 * @returns
	 */
	async runJobProcessor(workflow) {
		const client = await this.isClientReady(workflow);
		if (client.stopped) {
			this.log("warn", workflow.name, null, "Job processor is stopped");
			return;
		}
		try {
			client.blocked = true;
			const jobId = await client.brpoplpush(
				this.getKey(workflow.name, C.QUEUE_WAITING),
				this.getKey(workflow.name, C.QUEUE_ACTIVE),
				this.opts.drainDelay
			);
			client.blocked = false;

			if (jobId) {
				await this.processJob(workflow, jobId);
			} else {
				// this.logger.debug(`No job for workflow '${workflow.name}'...`);
			}
		} catch (err) {
			// Swallow error if disconnecting
			if (!this.disconnecting) {
				this.log("error", workflow.name, null, "Unable to watch job", err);
			}
		} finally {
			setImmediate(() => this.runJobProcessor(workflow));
		}
	}

	/**
	 * Set lock for the job.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @returns
	 */
	async lock(workflow, jobId) {
		// Set lock
		const lockRes = await this.commandClient.set(
			this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId),
			this.broker.instanceID,
			"PX",
			this.opts.lockDuration,
			"NX"
		);
		this.log("debug", workflow.name, jobId, "Lock result", lockRes);

		if (!lockRes) throw new Error(`Job ${jobId} is already locked.`);

		const lockExtender = async () => {
			this.log("debug", workflow.name, jobId, "Extending lock");
			const lockRes = await this.commandClient.set(
				this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId),
				this.broker.instanceID,
				"PX",
				this.opts.lockDuration,
				"XX"
			);
			if (!lockRes) {
				this.log("debug", workflow.name, jobId, "Job lock is expired");
				return;
			}
		};

		// Start lock extender
		const timer = setInterval(() => lockExtender(), this.opts.lockDuration / 2);

		return async () => {
			clearInterval(timer);
			const unlockRes = await this.commandClient.del(
				this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId)
			);
			this.log("debug", workflow.name, jobId, "Unlock result", unlockRes);
		};
	}

	/**
	 * Get Job details and run it.
	 *
	 * @param {Workflow} workflow
	 * @param {String} jobId
	 */
	async processJob(workflow, jobId) {
		const job = await this.getJob(workflow, jobId);
		if (!job) {
			this.log("warn", workflow.name, jobId, "Job not found");
			// Remove from active queue
			await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);
			return;
		}

		const jobEvents = await this.getJobEvents(workflow, jobId);

		const unlock = await this.lock(workflow, jobId);
		try {
			await this.commandClient.hset(
				this.getKey(workflow.name, C.QUEUE_JOB, jobId),
				"startedAt",
				Date.now()
			);

			this.log("debug", workflow.name, jobId, "Running job...", job);

			const result = await this.callWorkflowHandler(workflow, job, jobEvents);

			this.log("info", workflow.name, jobId, "Job finished.", result);
			await this.moveToCompleted(workflow, jobId, result);
		} catch (err) {
			this.log("error", workflow.name, jobId, "Job processing is failed.", err);
			await this.moveToFailed(workflow, jobId, err);
		} finally {
			await unlock();
		}
	}

	/**
	 * Get job from Redis.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @returns
	 */
	async getJob(workflow, jobId) {
		const payload = await this.commandClient.hget(
			this.getKey(workflow.name, C.QUEUE_JOB, jobId),
			"payload"
		);

		return {
			id: jobId,
			payload: payload != null ? this.serializer.deserialize(payload) : null
		};
	}

	/**
	 * Get job events from Redis.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @returns
	 */
	async getJobEvents(workflow, jobId) {
		const jobEvents = await this.commandClient.lrange(
			this.getKey(workflow.name, C.QUEUE_EVENTS, jobId),
			0,
			-1
		);

		this.log("debug", workflow.name, jobId, "Job events:", jobEvents);
		return jobEvents.map(e => this.serializer.deserialize(e));
	}

	/**
	 * Add job event to Redis.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @param {*} event
	 * @returns
	 */
	async addJobEvent(workflow, jobId, event) {
		await this.commandClient.rpush(
			this.getKey(workflow.name, C.QUEUE_EVENTS, jobId),
			this.serializer.serialize({
				...event,
				ts: Date.now(),
				nodeID: this.broker.nodeID
			})
		);
	}

	/**
	 * Move job to completed queue.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @param {*} result
	 */
	async moveToCompleted(workflow, jobId, result) {
		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);
		if (this.opts.removeOnComplete) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, jobId));
		} else {
			const fields = {
				success: true,
				completedAt: Date.now()
			};
			if (result != null) {
				fields.result = this.serializer.serialize(result);
			}

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, jobId), fields);

			// Push to completed queue
			await this.commandClient.zadd(
				this.getKey(workflow.name, C.QUEUE_COMPLETED),
				fields.completedAt,
				jobId
			);
		}
	}

	getBackoffTime(retryAttempts) {
		const backoff = this.opts.backoff;
		const delay = parseDuration(this.opts.backoffDelay) ?? 100;
		if (typeof backoff === "function") {
			return backoff(retryAttempts);
		} else if (backoff === "fixed") {
			return delay;
		} else if (backoff === "exponential") {
			return Math.pow(2, retryAttempts) * delay;
		}
	}

	/**
	 * Move job to failed queue.
	 *
	 * @param {Workflow} workflow
	 * @param {*} jobId
	 * @param {*} err
	 */
	async moveToFailed(workflow, jobId, err) {
		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);

		if (err?.retryable) {
			let retryFields = await this.commandClient.hmget(
				this.getKey(workflow.name, C.QUEUE_JOB, jobId),
				["retries", "retryAttempts"]
			);
			const retries = parseInt(retryFields[0] ?? 0);
			const retryAttempts = parseInt(retryFields[1] ?? 0);

			if (retries > 0 && retryAttempts < retries) {
				await this.commandClient.hincrby(
					this.getKey(workflow.name, C.QUEUE_JOB, jobId),
					"retryAttempts",
					1
				);
				const backoffTime = this.getBackoffTime(retryAttempts);
				this.log(
					"debug",
					workflow.name,
					jobId,
					`Retrying job after ${backoffTime} ms...`,
					retryAttempts,
					retries
				);

				const promoteAt = Date.now() + backoffTime;
				await this.commandClient.hset(
					this.getKey(workflow.name, C.QUEUE_JOB, jobId),
					"promoteAt",
					promoteAt
				);
				await this.commandClient.zadd(
					this.getKey(workflow.name, C.QUEUE_DELAYED),
					promoteAt,
					jobId
				);

				return;
			}
		}

		if (this.opts.removeOnFailed) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, jobId));
		} else {
			const fields = {
				success: false,
				failedAt: Date.now()
			};
			if (err != null) {
				fields.error = this.serializer.serialize(
					this.broker.errorRegenerator.extractPlainError(err)
				);
			}

			this.log("debug", workflow.name, jobId, `Job move to failed queue.`);

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, jobId), fields);

			// Push to failed queue
			await this.commandClient.zadd(
				this.getKey(workflow.name, C.QUEUE_FAILED),
				fields.failedAt,
				jobId
			);
		}
	}

	/**
	 * Save state of a job.
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 * @param {*} event
	 * @returns
	 */
	async saveJobState(workflow, jobId, state) {
		await this.commandClient.hset(
			this.getKey(workflow.name, C.QUEUE_JOB, jobId),
			"state",
			this.serializer.serialize(state)
		);
		this.log("debug", workflow.name, jobId, "Job state set.", state);
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param {string} signalName
	 * @param {unknown} key
	 * @param {unknown} payload
	 * @returns
	 */
	async triggerSignal(signalName, key, payload) {
		const content = this.serializer.serialize({
			signal: signalName,
			key,
			payload
		});

		this.log("debug", null, null, "Trigger signal", content.toString());

		const exp = parseDuration(this.opts.signalExpiration);
		if (exp != null && exp > 0) {
			await this.commandClient.set(
				this.getKey(C.QUEUE_SIGNAL, signalName, key),
				content,
				"NX",
				"PX",
				exp
			);
		} else {
			await this.commandClient.set(
				this.getKey(C.QUEUE_SIGNAL, signalName, key),
				content,
				"NX"
			);
		}

		await this.commandClient.publish(this.getKey(C.QUEUE_SIGNAL), content);
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param {string} signalName
	 * @param {unknown} key
	 * @param {unknown} opts
	 * @returns payload
	 */
	async waitForSignal(signalName, key, opts) {
		const content = await this.commandClient.get(this.getKey(C.QUEUE_SIGNAL, signalName, key));
		if (content) {
			const json = this.serializer.deserialize(content);
			if (key === json.key) {
				this.logger.debug("Signal received", signalName, key, json);
				return json.payload;
			}
		}

		const pKey = signalName + ":" + key;
		const found = this.signalPromises.get(pKey);
		if (found) {
			return found.promise;
		}

		const item = {};
		item.promise = new Promise(resolve => (item.resolve = resolve));
		this.signalPromises.set(pKey, item);
		this.logger.debug("Waiting for signal", signalName, key);

		return item.promise;
	}

	/**
	 * Create a new job and push it to the waiting queue.
	 *
	 * @param {Workflow} workflowName
	 * @param {*} payload
	 * @param {*} opts
	 * @returns
	 */
	async createJob(workflowName, payload, opts) {
		opts = opts || {};

		const jobId = opts.jobId ?? this.broker.generateUid();

		const job = {
			id: jobId,
			payload: this.serializer.serialize(payload),
			createdAt: Date.now()
		};

		if (opts.retries) {
			job.retries = opts.retries;
			job.retryAttempts = 0;
		}

		if (opts.timeout) {
			job.timeout = parseDuration(opts.timeout);
		}

		if (opts.delay) {
			job.delay = parseDuration(opts.delay);
			job.promoteAt = Date.now() + job.delay;
		}

		await this.commandClient.hmset(this.getKey(workflowName, C.QUEUE_JOB, jobId), job);
		this.log("debug", workflowName, job.id, "Job created.", job);

		if (job.delay) {
			await this.commandClient.zadd(
				this.getKey(workflowName, C.QUEUE_DELAYED),
				job.promoteAt,
				jobId
			);
		} else {
			await this.commandClient.rpush(this.getKey(workflowName, C.QUEUE_WAITING), jobId);
		}

		return job;
	}

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 * If both are provided, the adapter should clean up only the job with the given ID.
	 * If only the workflow name is provided, the adapter should clean up all jobs
	 * related to that workflow.
	 * If neither is provided, the adapter should clean up all jobs.
	 *
	 * @param {string?} workflowName
	 * @param {string?} jobId
	 */
	async cleanUp(workflowName, jobId) {
		if (workflowName && jobId) {
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB, jobId));
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB_LOCK, jobId));
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId));
			this.log("info", workflowName, jobId, "Cleaned up job store.");
		} else if (workflowName) {
			await this.cleanDb(this.getKey(workflowName) + ":*");
			this.log("info", workflowName, null, "Cleaned up workflow store.");
		} else {
			await this.cleanDb(this.prefix + ":*");
			this.logger.info(`Cleaned up entire store.`);
		}
	}

	async cleanDb(pattern) {
		if (this.commandClient instanceof Redis.Cluster) {
			return this.clusterCleanDb(pattern);
		} else {
			return this.nodeCleanDb(this.commandClient, pattern);
		}
	}

	async clusterCleanDb(pattern) {
		// get only master nodes to scan for deletion,
		// if we get slave nodes, it would be failed for deletion.
		return this.commandClient
			.nodes("master")
			.map(async node => this.nodeScanDel(node, pattern));
	}

	async nodeCleanDb(node, pattern) {
		return new Promise((resolve, reject) => {
			const stream = node.scanStream({
				match: pattern,
				count: 100
			});

			stream.on("data", (keys = []) => {
				if (!keys.length) {
					return;
				}

				stream.pause();
				node.del(keys)
					.then(() => {
						stream.resume();
					})
					.catch(err => {
						err.pattern = pattern;
						return reject(err);
					});
			});

			stream.on("error", err => {
				this.logger.error(
					`Error occured while deleting keys '${pattern}' from Redis node.`,
					err
				);
				reject(err);
			});

			stream.on("end", () => {
				// End deleting keys from node
				resolve();
			});
		});
	}

	/**
	 *
	 * @param {*} workflow
	 */
	async lockMaintenance(workflow) {
		// Set lock
		const lockRes = await this.commandClient.set(
			this.getKey(workflow.name, C.QUEUE_MAINTENANCE_LOCK),
			this.broker.instanceID,
			"EX",
			this.opts.maintenanceTime * 2,
			"NX"
		);
		this.log("debug", workflow.name, null, "Lock maintenance", lockRes);

		return lockRes != null;
	}

	/**
	 *
	 * @param {*} workflow
	 */
	async maintenanceDelayedJobs(workflow) {
		this.log("debug", workflow.name, null, "Maintenance delayed jobs...");
		try {
			const now = Date.now();
			const jobIds = await this.commandClient.zrangebyscore(
				this.getKey(workflow.name, C.QUEUE_DELAYED),
				0,
				now
			);
			if (jobIds.length > 0) {
				this.log(
					"debug",
					workflow.name,
					null,
					`Found ${jobIds.length} delayed jobs:`,
					jobIds
				);
				try {
					await this.commandClient.lpush(
						this.getKey(workflow.name, C.QUEUE_WAITING),
						jobIds
					);
					await this.commandClient.zrem(
						this.getKey(workflow.name, C.QUEUE_DELAYED),
						jobIds
					);
				} catch (err) {
					this.log(
						"error",
						workflow.name,
						null,
						"Error while moving delayed jobs to waiting queue",
						err
					);
				}
			}
		} catch (err) {
			this.log("error", workflow.name, null, "Error while processing delayed jobs", err);
		}
	}

	/**
	 *
	 * @param {*} workflow
	 */
	async maintenanceStalledJobs(workflow) {
		this.log("debug", workflow.name, null, "Maintenance stalled jobs...");

		try {
			const activeJobIds = await this.commandClient.lrange(
				this.getKey(workflow.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				for (const jobId of activeJobIds) {
					try {
						const exists = await this.commandClient.exists(
							this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId)
						);
						if (exists == 0) {
							await this.moveStalledJobsToFailed(workflow, jobId);
						}
					} catch (err) {
						this.log(
							"error",
							workflow.name,
							jobId,
							"Error while processing stalled job",
							err
						);
					}
				}
			}
		} catch (err) {
			this.log("error", workflow.name, null, "Error while processing stalled jobs", err);
		}
	}

	/**
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 */
	async moveStalledJobsToFailed(workflow, jobId) {
		const res = await this.commandClient.lrem(
			this.getKey(workflow.name, C.QUEUE_ACTIVE),
			1,
			jobId
		);
		// Check if the job was removed from the active queue (maybe someone else removed it already)
		if (res > 0) {
			await this.addJobEvent(workflow, jobId, { type: "stalled" });

			this.log("debug", workflow.name, jobId, `Job is stalled. Moved back to waiting queue.`);

			await this.commandClient.lpush(this.getKey(workflow.name, C.QUEUE_WAITING), jobId);
		}
	}

	/**
	 *
	 * @param {*} workflow
	 * @param {*} queueName
	 * @param {*} duration
	 */
	async maintenanceRemoveOldJobs(workflow, queueName, duration) {
		this.log("debug", workflow.name, null, `Maintenance remove old ${queueName} jobs...`);

		try {
			const minDate = Date.now() - duration;
			const jobIds = await this.commandClient.zrangebyscore(
				this.getKey(workflow.name, queueName),
				0,
				minDate
			);
			if (jobIds.length > 0) {
				this.log(
					"debug",
					workflow.name,
					null,
					`Found ${jobIds.length} completed jobs:`,
					jobIds
				);
				const jobKeys = jobIds.map(jobId => this.getKey(workflow.name, C.QUEUE_JOB, jobId));
				const jobEventsKeys = jobIds.map(jobId =>
					this.getKey(workflow.name, C.QUEUE_EVENTS, jobId)
				);
				await this.commandClient.del(jobKeys);
				await this.commandClient.del(jobEventsKeys);
				await this.commandClient.zremrangebyscore(
					this.getKey(workflow.name, queueName),
					0,
					minDate
				);

				this.log(
					"debug",
					workflow.name,
					null,
					`Removed ${jobIds.length} ${queueName} jobs:`,
					jobIds
				);
			}
		} catch (err) {
			this.log(
				"error",
				workflow.name,
				null,
				`Error while removing old ${queueName} jobs`,
				err
			);
		}
	}

	/**
	 *
	 * @param {*} workflow
	 */
	async unlockMaintenance(workflow) {
		const unlockRes = await this.commandClient.del(
			this.getKey(workflow.name, C.QUEUE_MAINTENANCE_LOCK)
		);
		this.log("debug", workflow.name, null, "Unlock maintenance", unlockRes);
	}

	/**
	 * Create Redis standalone or cluster client.
	 *
	 * @returns
	 */
	createRedisClient() {
		let client;

		const opts = this.opts.redis;

		if (opts && opts.cluster) {
			if (!opts.cluster.nodes || opts.cluster.nodes.length === 0) {
				throw new MoleculerError(
					"No nodes defined for Redis cluster in Workflow adapter.",
					500,
					"ERR_NO_REDIS_CLUSTER_NODES"
				);
			}
			client = new Redis.Cluster(opts.cluster.nodes, opts.cluster.clusterOptions);
		} else {
			client = new Redis(opts && opts.url ? opts.url : opts);
		}

		return client;
	}

	/**
	 * Get Redis key for the given name and type.
	 *
	 * @param {string} name
	 * @param {string} type
	 * @param {string?} id
	 * @returns
	 */
	getKey(name, type, id) {
		if (id) {
			return `${this.prefix}${name}:${type}:${id}`;
		} else if (type) {
			return `${this.prefix}${name}:${type}`;
		} else {
			return `${this.prefix}${name}`;
		}
	}
}

module.exports = RedisAdapter;
