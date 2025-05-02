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

	log(level, workflowName, jobId, msg, ...args) {
		if (this.logger) {
			this.logger[level](`[${workflowName}:${jobId}] ${msg}`, ...args);
		}
	}

	/**
	 * Connect to Redis.
	 */
	async connect() {
		if (this.connected) return;

		await this.createCommandClient();

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

		this.disconnecting = false;
	}

	/**
	 * Wait for Redis client to be ready. If it's not exists, it will create a new one.
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
			// this.logger.debug(
			// 	`Waiting for job in workflow '${workflow.name}'...`,
			// 	this.getKey(workflow.name, C.QUEUE_WAITING)
			// );
			client.blocked = true;
			const jobId = await client.brpoplpush(
				this.getKey(workflow.name, C.QUEUE_WAITING),
				this.getKey(workflow.name, C.QUEUE_ACTIVE),
				this.opts.drainDelay
			);
			client.blocked = false;

			if (jobId) {
				this.log("debug", workflow.name, jobId, "Job is found");
				await this.processJob(workflow, jobId);
			} else {
				// this.logger.debug(`No job for workflow '${workflow.name}'...`);
			}
		} catch (err) {
			// Swallow error if disconnecting
			if (!this.disconnecting) {
				this.log("error", workflow.name, null, "Unable to watch job", err);
			}
		}
		setImmediate(() => this.runJobProcessor(workflow));
	}

	/**
	 * Set lock for the job.
	 *
	 * @param {*} workflow
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
			this.log("debug", workflow.name, jobId, "Running job...", job);

			const result = await this.callWorkflowHandler(workflow, job, jobEvents);

			this.log("info", workflow.name, jobId, "Job finished.", result);
			await unlock();
			await this.moveToCompleted(workflow, jobId, result);
		} catch (err) {
			this.log("error", workflow.name, jobId, "Job processing is failed.", err);
			await unlock();
			await this.moveToFailed(workflow, jobId, err);
		}
	}

	/**
	 * Get job from Redis.
	 *
	 * @param {*} workflow
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
			payload: payload != null ? JSON.parse(payload) : null
		};
	}

	/**
	 * Get job events from Redis.
	 *
	 * @param {
	 * } workflow
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
		return jobEvents;
	}

	/**
	 * Add job event to Redis.
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 * @param {*} event
	 * @returns
	 */
	async addJobEvent(workflow, jobId, event) {
		await this.commandClient.rpush(
			this.getKey(workflow.name, C.QUEUE_EVENTS, jobId),
			JSON.stringify(event)
		);
	}

	/**
	 * Move job to completed queue.
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 * @param {*} result
	 */
	async moveToCompleted(workflow, jobId, result) {
		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);
		if (this.opts.removeOnComplete) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, jobId));
		} else {
			const fields = {
				completedAt: Date.now()
			};
			if (result != null) {
				fields.result = JSON.stringify(result);
			}

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, jobId), fields);

			// Push to completed queue
			await this.commandClient.rpush(this.getKey(workflow.name, C.QUEUE_COMPLETED), jobId);
		}
	}

	/**
	 * Move job to failed queue.
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 * @param {*} err
	 */
	async moveToFailed(workflow, jobId, err) {
		await this.commandClient.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, jobId);

		if (this.opts.removeOnFailed) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, jobId));
		} else {
			const fields = {
				failedAt: Date.now()
			};
			if (err != null) {
				fields.error = JSON.stringify({
					name: err.name,
					message: err.message,
					stack: err.stack,
					code: err.code,
					type: err.type
				});
			}

			// Update job
			await this.commandClient.hmset(this.getKey(workflow.name, C.QUEUE_JOB, jobId), fields);

			// Push to failed queue
			await this.commandClient.rpush(this.getKey(workflow.name, C.QUEUE_FAILED), jobId);
		}
	}

	/**
	 * Create a new job and push it to the waiting queue.
	 *
	 * @param {*} workflowName
	 * @param {*} payload
	 * @param {*} opts
	 * @returns
	 */
	async createJob(workflowName, payload, opts) {
		opts = opts || {};

		const jobId = opts.jobId ?? this.broker.generateUid();

		const job = {
			id: jobId,
			payload: JSON.stringify(payload),
			createdAt: Date.now()
		};

		// if (opts.retries) {
		// 	job.retries = opts.retries;
		// }
		// if (opts.timeout) {
		// 	job.timeout = opts.timeout;
		// }
		// if (opts.delay) {
		// 	job.delay = opts.delay;
		// }

		await this.commandClient.hmset(this.getKey(workflowName, C.QUEUE_JOB, jobId), job);
		this.log("debug", workflowName, job.id, "Job created.", job);

		await this.commandClient.rpush(this.getKey(workflowName, C.QUEUE_WAITING), jobId);

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
			this.logger.info(`Cleaned up Redis adapter store for workflow '${workflowName}'`);
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
