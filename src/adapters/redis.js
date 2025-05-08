/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const BaseAdapter = require("./base");
const { BrokerOptionsError } = require("moleculer").Errors;
const { WorkflowError, WorkflowAlreadyLocked } = require("../errors");
const C = require("../constants");
const Redis = require("ioredis");
const { parseDuration, humanize, getCronNextTime } = require("../utils");

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
 * @typedef {import("../index.d.ts").RedisAdapterOptions} RedisAdapterOptions
 */
class RedisAdapter extends BaseAdapter {
	/**
	 * Constructor of adapter.
	 *
	 * @param  {RedisAdapterOptions?} opts
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
		this.jobResultPromises = new Map();

		this.disconnecting = false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {ServiceBroker} broker - The Moleculer Service Broker instance.
	 * @param {LoggerInstance} logger - The logger instance.
	 */
	init(broker, logger, mixinOpts) {
		super.init(broker, logger, mixinOpts);

		if (this.opts.prefix) {
			this.prefix = this.opts.prefix + ":";
		} else if (this.broker.namespace) {
			this.prefix = "molwf-" + this.broker.namespace + ":";
		} else {
			this.prefix = "molwf:";
		}

		this.logger.info("Workflows Redis adapter prefix:", this.prefix);
	}

	/**
	 * Connect to Redis.
	 *
	 * @returns {Promise<void>} Resolves when the connection is established.
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
				} else if (
					channel.startsWith(this.prefix) &&
					channel.endsWith(C.FINISHED) &&
					this.jobResultPromises.size > 0
				) {
					const json = this.serializer.deserialize(message);
					this.logger.debug("Job finished message received.", json);
					const jobId = json.jobId;
					if (this.jobResultPromises.has(jobId)) {
						const storePromise = this.jobResultPromises.get(jobId);
						this.jobResultPromises.delete(jobId);
						if (json.error) {
							storePromise.reject(
								this.broker.errorRegenerator.extractPlainError(json.error)
							);
						} else {
							storePromise.resolve(json.result);
						}
					}
				}
			});
		});
	}

	/**
	 * Disconnect from the adapter.
	 *
	 * @returns {Promise<void>} Resolves when the disconnection is complete.
	 */
	async destroy() {
		if (this.disconnecting) return;

		this.disconnecting = true;
		this.connected = false;

		await super.destroy();

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
	 * Wait for Redis client to be ready. If it does not exist, it will create a new one.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @returns {Promise<Redis>} Resolves with the Redis client instance.
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

	/**
	 * Start the job processor for the given workflow.
	 *
	 * @param {string} workflow - The name of the workflow.
	 */
	startJobProcessor(workflow) {
		if (!this.jobClients[workflow.name]) {
			this.runJobProcessor(workflow);
		}
	}

	/**
	 * Stop the job processor for the given workflow.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @returns {Promise<void>} Resolves when the job processor is stopped.
	 */
	async stopJobProcessor(workflow) {
		if (this.jobClients[workflow.name]) {
			await this.jobClients[workflow.name].quit();
			this.jobClients[workflow.name].stopped = true;
		}
	}

	/**
	 * Job processor for the given workflow. Waits for a job in the waiting queue, moves it to the active queue, and processes it.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @returns {Promise<void>} Resolves when the job is processed.
	 */
	async runJobProcessor(workflow) {
		const client = await this.isClientReady(workflow.name);
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
	 * Set a lock for the job.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {string} jobId - The ID of the job to lock.
	 * @returns {Promise<Function>} Resolves with a function to unlock the job.
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

		if (!lockRes) throw new WorkflowError(`Job ${jobId} is already locked.`);

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
	 * Get job details and process it.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {string} jobId - The ID of the job to process.
	 * @returns {Promise<void>} Resolves when the job is processed.
	 */
	async processJob(workflow, jobId) {
		const job = await this.getJob(workflow.name, jobId);
		if (!job) {
			this.log("warn", workflow.name, jobId, "Job not found");
			// Remove from active queue
			await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);
			return;
		}

		const jobEvents = await this.getJobEvents(workflow.name, jobId);

		let unlock;
		try {
			unlock = await this.lock(workflow, jobId);

			const now = Date.now();
			await this.commandClient.hset(
				this.getKey(workflow.name, C.QUEUE_JOB, jobId),
				"startedAt",
				now
			);

			await this.addJobEvent(workflow.name, job.id, {
				type: "started"
			});

			this.sendJobEvent(workflow.name, job.id, "started");

			this.log("debug", workflow.name, jobId, "Running job...", job);

			const result = await this.callWorkflowHandler(workflow, job, jobEvents);

			const duration = Date.now() - now;
			this.log(
				"debug",
				workflow.name,
				jobId,
				`Job finished in ${humanize(duration)}.`,
				result
			);

			await this.moveToCompleted(workflow, job, result);
		} catch (err) {
			if (err instanceof WorkflowAlreadyLocked) {
				this.log("debug", workflow.name, jobId, "Job is already locked.");
				return;
			}
			this.log("error", workflow.name, jobId, "Job processing is failed.", err);
			await this.moveToFailed(workflow, job, err);
		} finally {
			if (unlock) await unlock();
		}
	}

	/**
	 * Get a job from Redis.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {string[]|boolean} fields - The fields to retrieve or true to retrieve all fields.
	 * @returns {Promise<Object|null>} Resolves with the job object or null if not found.
	 */
	async getJob(workflowName, jobId, fields) {
		if (fields == null) {
			fields = ["payload", "parent"];
		}

		const exists = await this.commandClient.exists(
			this.getKey(workflowName, C.QUEUE_JOB, jobId)
		);
		if (!exists) return null;

		let job;

		if (fields === true) {
			job = await this.commandClient.hgetall(this.getKey(workflowName, C.QUEUE_JOB, jobId));
		} else {
			const values = await this.commandClient.hmget(
				this.getKey(workflowName, C.QUEUE_JOB, jobId),
				fields
			);

			job = fields.reduce((acc, field, i) => {
				if (values[i] == null) return acc;
				acc[field] = values[i];
				return acc;
			}, {});

			job.id = jobId;
		}

		return this.deserializeJob(job);
	}

	/**
	 * Get job events from Redis.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @returns {Promise<Object[]>} Resolves with an array of job events.
	 */
	async getJobEvents(workflowName, jobId) {
		const jobEvents = await this.commandClient.lrange(
			this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId),
			0,
			-1
		);

		return jobEvents.map(e => this.serializer.deserialize(e));
	}

	/**
	 * Add a job event to Redis.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {Object} event - The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	async addJobEvent(workflowName, jobId, event) {
		await this.commandClient.rpush(
			this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId),
			this.serializer.serialize({
				...event,
				ts: Date.now(),
				nodeID: this.broker.nodeID
			})
		);
	}

	/**
	 * Move a job to the completed queue.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {Object} job - The job object.
	 * @param {*} result - The result of the job execution.
	 * @returns {Promise<void>} Resolves when the job is moved to the completed queue.
	 */
	async moveToCompleted(workflow, job, result) {
		await this.addJobEvent(workflow.name, job.id, {
			type: "finished"
		});

		this.sendJobEvent(workflow.name, job.id, "finished");
		this.sendJobEvent(workflow.name, job.id, "completed");

		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, job.id);
		if (this.opts.removeOnComplete) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, job.id));
		} else {
			const fields = {
				success: true,
				finishedAt: Date.now()
			};
			if (result != null) {
				fields.result = this.serializer.serialize(result);
			}

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, job.id), fields);

			// Push to completed queue
			await this.commandClient.zadd(
				this.getKey(workflow.name, C.QUEUE_COMPLETED),
				fields.finishedAt,
				job.id
			);
		}

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.resolve(result);
		} else {
			this.commandClient.publish(
				this.getKey(workflow.name, C.FINISHED),
				this.serializer.serialize({
					jobId: job.id,
					result
				})
			);
		}

		if (job.parent) {
			await this.rescheduleJob(workflow.name, job.parent);
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
	 * Move a job to the failed queue.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {Object|string} job - The job object.
	 * @param {Error} err - The error that caused the job to fail.
	 * @returns {Promise<void>} Resolves when the job is moved to the failed queue.
	 */
	async moveToFailed(workflow, job, err) {
		if (typeof job == "string") {
			job = await this.getJob(workflow.name, job, ["parent"]);
		}

		await this.addJobEvent(workflow.name, job.id, {
			type: "failed",
			error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
		});

		this.sendJobEvent(workflow.name, job.id, "finished");
		this.sendJobEvent(workflow.name, job.id, "failed");

		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, job.id);

		if (err?.retryable) {
			let retryFields = await this.commandClient.hmget(
				this.getKey(workflow.name, C.QUEUE_JOB, job.id),
				["retries", "retryAttempts"]
			);
			const retries = parseInt(retryFields[0] ?? 0);
			const retryAttempts = parseInt(retryFields[1] ?? 0);

			if (retries > 0 && retryAttempts < retries) {
				await this.commandClient.hincrby(
					this.getKey(workflow.name, C.QUEUE_JOB, job.id),
					"retryAttempts",
					1
				);
				const backoffTime = this.getBackoffTime(retryAttempts);
				this.log(
					"debug",
					workflow.name,
					job.id,
					`Retrying job after ${backoffTime} ms...`,
					retryAttempts,
					retries
				);

				const promoteAt = Date.now() + backoffTime;
				await this.commandClient.hset(
					this.getKey(workflow.name, C.QUEUE_JOB, job.id),
					"promoteAt",
					promoteAt
				);
				await this.commandClient.zadd(
					this.getKey(workflow.name, C.QUEUE_DELAYED),
					promoteAt,
					job.id
				);

				return;
			}
		}

		if (this.opts.removeOnFailed) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, job.id));
		} else {
			const fields = {
				success: false,
				finishedAt: Date.now()
			};
			if (err != null) {
				fields.error = this.serializer.serialize(
					this.broker.errorRegenerator.extractPlainError(err)
				);
			}

			this.log("debug", workflow.name, job.id, `Job move to failed queue.`);

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, job.id), fields);

			// Push to failed queue
			await this.commandClient.zadd(
				this.getKey(workflow.name, C.QUEUE_FAILED),
				fields.finishedAt,
				job.id
			);
		}

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.reject(err);
		} else {
			this.commandClient.publish(
				this.getKey(workflow.name, C.FINISHED),
				this.serializer.serialize({
					jobId: job.id,
					error: this.broker.errorRegenerator.extractPlainError(err)
				})
			);
		}

		if (job.parent) {
			await this.rescheduleJob(workflow.name, job.parent);
		}
	}

	/**
	 * Save the state of a job.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {Object} state - The state object to save.
	 * @returns {Promise<void>} Resolves when the state is saved.
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
	 * Get state of a workflow run.
	 *
	 * @param {string} workflowName
	 * @param {string} jobId
	 * @returns
	 */
	async getState(workflowName, jobId) {
		const state = await this.commandClient.hget(
			this.getKey(workflowName, C.QUEUE_JOB, jobId),
			"state"
		);

		return state != null ? this.serializer.deserialize(state) : null;
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param {string} signalName - The name of the signal.
	 * @param {unknown} key - The key associated with the signal.
	 * @param {unknown} payload - The payload of the signal.
	 * @returns {Promise<void>} Resolves when the signal is triggered.
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
	 * Remove a named signal.
	 *
	 * @param {string} signalName - The name of the signal.
	 * @param {unknown} key - The key associated with the signal.
	 * @returns {Promise<void>} Resolves when the signal is triggered.
	 */
	async removeSignal(signalName, key) {
		this.log("debug", null, null, "Remove signal", signalName, key);

		await this.commandClient.del(this.getKey(C.QUEUE_SIGNAL, signalName, key));
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param {string} signalName - The name of the signal.
	 * @param {unknown} key - The key associated with the signal.
	 * @param {unknown} opts - Additional options for waiting.
	 * @returns {Promise<unknown>} Resolves with the signal payload.
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
	 * Create a new job and push it to the waiting or delayed queue.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {*} payload - The payload of the job.
	 * @param {*} opts - Additional options for the job.
	 * @returns {Promise<Object>} Resolves with the created job object.
	 */
	async createJob(workflowName, payload, opts) {
		opts = opts || {};

		const jobId = this.checkJobId(opts.jobId) ?? this.broker.generateUid();

		const job = {
			id: jobId,
			payload,
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

		if (opts.repeat) {
			if (!opts.jobId) {
				throw new WorkflowError(
					"Job ID is required for repeatable jobs",
					400,
					"MISSING_REPEAT_JOB_ID"
				);
			}

			job.repeat = opts.repeat;
			job.repeatCounter = 0;

			if (opts.repeat.endDate) {
				const endDate = new Date(opts.repeat.endDate).getTime();
				if (endDate < Date.now()) {
					throw new WorkflowError(
						"Repeatable job is expired at " + opts.repeat.endDate,
						400,
						"REPEAT_JOB_EXPIRED",
						{
							jobId,
							endDate: opts.repeat.endDate
						}
					);
				}
			}
		}

		// Save the Job to Redis
		await this.commandClient.hmset(
			this.getKey(workflowName, C.QUEUE_JOB, jobId),
			this.serializeJob(job)
		);

		if (job.repeat) {
			// Repeatable job
			this.log("debug", workflowName, job.id, "Repeatable job created.", job);

			await this.rescheduleJob(workflowName, job);
		} else if (job.delay) {
			// Delayed job
			this.log(
				"debug",
				workflowName,
				job.id,
				`Delayed job created. Next run: ${new Date(job.promoteAt).toISOString()}`,
				job
			);
			await this.commandClient.zadd(
				this.getKey(workflowName, C.QUEUE_DELAYED),
				job.promoteAt,
				jobId
			);
		} else {
			// Normal job
			this.log("debug", workflowName, job.id, "Job created.", job);
			await this.commandClient.rpush(this.getKey(workflowName, C.QUEUE_WAITING), jobId);
		}

		this.sendJobEvent(workflowName, job.id, "created");

		job.promise = () => {
			let storePromise = {};
			storePromise.promise = new Promise((resolve, reject) => {
				storePromise.resolve = resolve;
				storePromise.reject = reject;
			});

			this.jobResultPromises.set(jobId, storePromise);

			this.signalSubClient?.subscribe(this.getKey(workflowName, C.FINISHED));

			return storePromise.promise;
		};

		return job;
	}

	/**
	 * Serialize a job object for storage in Redis.
	 *
	 * @param {Object} job - The job object to serialize.
	 * @returns {Object} The serialized job object.
	 */
	serializeJob(job) {
		const res = { ...job };
		if (job.payload) {
			res.payload = this.serializer.serialize(job.payload);
		}
		if (job.repeat) {
			res.repeat = this.serializer.serialize(job.repeat);
		}
		return res;
	}

	/**
	 * Deserialize a job object retrieved from Redis.
	 *
	 * @param {Object} job - The serialized job object.
	 * @returns {Object} The deserialized job object.
	 */
	deserializeJob(job) {
		const res = { ...job };
		if (job.payload != null) {
			res.payload = this.serializer.deserialize(job.payload);
		}
		if (job.repeat != null) {
			res.repeat = this.serializer.deserialize(job.repeat);
		}
		if (job.result != null) {
			res.result = this.serializer.deserialize(job.result);
		}
		if (job.error != null) {
			res.error = this.serializer.deserialize(job.error);
		}
		if (job.state != null) {
			res.state = this.serializer.deserialize(job.state);
		}
		if (job.createdAt != null) {
			res.createdAt = Number(job.createdAt);
		}
		if (job.startedAt != null) {
			res.startedAt = Number(job.startedAt);
		}
		if (job.finishedAt != null) {
			res.finishedAt = Number(job.finishedAt);
		}
		if (job.promoteAt != null) {
			res.promoteAt = Number(job.promoteAt);
		}
		if (job.repeatCounter != null) {
			res.repeatCounter = Number(job.repeatCounter);
		}
		if (job.stalledCounter != null) {
			res.stalledCounter = Number(job.stalledCounter);
		}
		return res;
	}

	/**
	 * Reschedule a repeatable job based on its configuration.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {Object|string} job - The job object or job ID to reschedule.
	 * @returns {Promise<void>} Resolves when the job is rescheduled.
	 */
	async rescheduleJob(workflowName, job) {
		try {
			if (typeof job == "string") {
				job = await this.getJob(workflowName, job, [
					"payload",
					"repeat",
					"repeatCounter",
					"retries",
					"timeout"
				]);

				if (!job) {
					this.log("warn", workflowName, job, "Parent job not found. Not rescheduling.");
					return;
				}
			}

			const nextJob = { ...job };
			delete nextJob.repeat;
			nextJob.parent = job.id;

			if (job.repeat.cron) {
				if (job.repeat.endDate) {
					const endDate = new Date(job.repeat.endDate).getTime();
					if (endDate < Date.now()) {
						this.log(
							"debug",
							workflowName,
							job.id,
							`Repeatable job is expired at ${job.repeat.endDate}. Not rescheduling.`,
							job
						);

						await this.commandClient.hset(
							this.getKey(workflowName, C.QUEUE_JOB, job.id),
							"finishedAt",
							Date.now()
						);
						return;
					}
				}
				if (job.repeat.limit > 0) {
					if (job.repeatCounter >= job.repeat.limit) {
						this.log(
							"debug",
							workflowName,
							job.id,
							`Repeatable job reached the limit of ${job.repeat.limit}. Not rescheduling.`,
							job
						);

						await this.commandClient.hset(
							this.getKey(workflowName, C.QUEUE_JOB, job.id),
							"finishedAt",
							Date.now()
						);

						return;
					}

					nextJob.repeatCounter = await this.commandClient.hincrby(
						this.getKey(workflowName, C.QUEUE_JOB, job.id),
						"repeatCounter",
						1
					);
				}

				const promoteAt = getCronNextTime(job.repeat.cron, Date.now(), job.repeat.tz);
				if (!nextJob.promoteAt || nextJob.promoteAt < promoteAt) {
					nextJob.promoteAt = promoteAt;
				}

				nextJob.id = job.id + ":" + nextJob.promoteAt;

				// Save the next scheduled Job to Redis
				await this.commandClient.hmset(
					this.getKey(workflowName, C.QUEUE_JOB, nextJob.id),
					this.serializeJob(nextJob)
				);
				this.log(
					"debug",
					workflowName,
					job.id,
					`Scheduled job created. Next run: ${new Date(nextJob.promoteAt).toISOString()}`,
					nextJob
				);

				// Push to delayed queue
				await this.commandClient.zadd(
					this.getKey(workflowName, C.QUEUE_DELAYED),
					nextJob.promoteAt,
					nextJob.id
				);
			}
		} catch (err) {
			this.log("error", workflowName, job.id, "Error while rescheduling job", err);
		}
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
	 * @returns {Promise<void>}
	 */
	async cleanUp(workflowName, jobId) {
		if (workflowName && jobId) {
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB, jobId));
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB_LOCK, jobId));
			await this.commandClient.del(this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId));
			await this.commandClient.lrem(this.getKey(workflowName, C.QUEUE_WAITING), 1, jobId);
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
	 * Acquire a maintenance lock for a workflow.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @returns {Promise<boolean>} Resolves with true if the lock is acquired, false otherwise.
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
	 * Process delayed jobs for a workflow and move them to the waiting queue if ready.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @returns {Promise<void>} Resolves when delayed jobs are processed.
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
				for (const jobId of jobIds) {
					try {
						const job = await this.getJob(workflow.name, jobId, ["id"]);
						if (job) {
							await this.commandClient.lpush(
								this.getKey(workflow.name, C.QUEUE_WAITING),
								jobId
							);
						}
						await this.commandClient.zrem(
							this.getKey(workflow.name, C.QUEUE_DELAYED),
							jobId
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
			}
		} catch (err) {
			this.log("error", workflow.name, null, "Error while processing delayed jobs", err);
		}
	}

	/**
	 * Process stalled jobs for a workflow and move them back to the waiting queue.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @returns {Promise<void>} Resolves when stalled jobs are processed.
	 */
	async maintenanceStalledJobs(workflow) {
		this.log("debug", workflow.name, null, "Maintenance stalled jobs...");

		try {
			const stalledJobIds = await this.commandClient.smembers(
				this.getKey(workflow.name, C.QUEUE_STALLED)
			);
			if (stalledJobIds.length > 0) {
				for (const jobId of stalledJobIds) {
					try {
						// Check the job lock is exists
						const exists = await this.commandClient.exists(
							this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId)
						);
						if (exists == 0) {
							// Check the job is in the active queue
							const removed = await this.commandClient.lrem(
								this.getKey(workflow.name, C.QUEUE_ACTIVE),
								1,
								jobId
							);
							if (removed > 0) {
								// Move the job back to the waiting queue
								await this.moveStalledJobsToWaiting(workflow, jobId);
							}
						}

						// Remove from stalled queue
						await this.commandClient.srem(
							this.getKey(workflow.name, C.QUEUE_STALLED),
							jobId
						);
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

			// Copy active jobIds to stalled queue
			const activeJobIds = await this.commandClient.lrange(
				this.getKey(workflow.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				await this.commandClient.sadd(
					this.getKey(workflow.name, C.QUEUE_STALLED),
					activeJobIds
				);
			}
		} catch (err) {
			this.log("error", workflow.name, null, "Error while processing stalled jobs", err);
		}
	}

	/**
	 * Move stalled job back to the waiting queue.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @param {string} jobId - The ID of the stalled job.
	 * @returns {Promise<void>} Resolves when the job is moved to the failed queue.
	 */
	async moveStalledJobsToWaiting(workflow, jobId) {
		const stalledCounter = await this.commandClient.hincrby(
			this.getKey(workflow.name, C.QUEUE_JOB, jobId),
			"stalledCounter",
			1
		);

		await this.addJobEvent(workflow.name, jobId, { type: "stalled" });
		this.sendJobEvent(workflow.name, jobId, "stalled");

		if (workflow.maxStalledCount > 0 && stalledCounter > workflow.maxStalledCount) {
			this.log(
				"debug",
				workflow.name,
				jobId,
				"Job is reached the maximum stalled count. It's moved to failed."
			);

			await this.moveToFailed(workflow, jobId, new Error("Job stalled too many times."));
			return;
		}

		this.log("debug", workflow.name, jobId, `Job is stalled. Moved back to waiting queue.`);
		await this.commandClient.lpush(this.getKey(workflow.name, C.QUEUE_WAITING), jobId);
	}

	/**
	 * Remove old jobs from a specified queue based on their age.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @param {string} queueName - The name of the queue (e.g., completed, failed).
	 * @param {number} duration - The age threshold in milliseconds for removing jobs.
	 * @returns {Promise<void>} Resolves when old jobs are removed.
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
					this.getKey(workflow.name, C.QUEUE_JOB_EVENTS, jobId)
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
	 * Release the maintenance lock for a workflow.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @returns {Promise<void>} Resolves when the lock is released.
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
	 * @returns {Redis|Cluster} A Redis client instance, either standalone or cluster.
	 * @throws {WorkflowError} If no nodes are defined for Redis cluster.
	 */
	createRedisClient() {
		let client;

		const opts = this.opts.redis;

		if (opts && opts.cluster) {
			if (!opts.cluster.nodes || opts.cluster.nodes.length === 0) {
				throw new BrokerOptionsError(
					"No nodes defined for Redis cluster in Workflow adapter.",
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
	 * @param {string} name - The name of the workflow or entity.
	 * @param {string} type - The type of the key (e.g., job, lock, events).
	 * @param {string} [id] - Optional ID to append to the key.
	 * @returns {string} The constructed Redis key.
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
