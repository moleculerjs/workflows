/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const BaseAdapter = require("./base");
const { BrokerOptionsError } = require("moleculer").Errors;
const { makeDirs } = require("moleculer").Utils;
const { WorkflowError, WorkflowAlreadyLocked, WorkflowTimeoutError } = require("../errors");
const C = require("../constants");
const Redis = require("ioredis");
const { parseDuration, humanize, getCronNextTime } = require("../utils");

/**
 * @typedef {import("ioredis").Cluster} Cluster Redis cluster instance. More info: https://github.com/luin/ioredis/blob/master/API.md#Cluster
 * @typedef {import("ioredis").Redis} Redis Redis instance. More info: https://github.com/luin/ioredis/blob/master/API.md#Redis
 * @typedef {import("ioredis").RedisOptions} RedisOptions
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("./base").BaseDefaultOptions} BaseDefaultOptions Base adapter options
 */

const SIGNAL_EMPTY_KEY = "null";

const JOB_FIELDS_JSON = ["payload", "repeat", "result", "error", "state"];

const JOB_FIELDS_NUMERIC = [
	"createdAt",
	"startedAt",
	"finishedAt",
	"promoteAt",
	"repeatCounter",
	"stalledCounter",
	"delay",
	"timeout",
	"duration",
	"retries",
	"retryAttempts"
];

const JOB_FIELDS_BOOLEAN = ["success"];

// const JOB_FIELDS_STRING = ["id", "parent", "nodeID"];

// const JOB_FIELDS = [...JOB_FIELDS_STRING, ...JOB_FIELDS_JSON, ...JOB_FIELDS_NUMERIC, ...JOB_FIELDS_BOOLEAN];

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
			redis: {
				retryStrategy: times => Math.min(times * 500, 5000)
			}
		});

		this.jobClients = new Map();
		this.commandClient = null;
		this.subClient = null;

		this.signalPromises = new Map();
		this.jobResultPromises = new Map();

		this.disconnecting = false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {ServiceBroker} broker - The Moleculer Service Broker instance.
	 * @param {LoggerInstance} logger - The logger instance.
	 * @param {WorkflowsMiddlewareOptions} mwOpts - Middleware options.
	 */
	init(broker, logger, mwOpts) {
		super.init(broker, logger, mwOpts);

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

	async afterConnected() {
		await super.afterConnected();
		// Start delayed maintenance timers for all registered workflows
		if (this.workflows) {
			for (const wf of this.workflows.values()) {
				await this.subClient?.subscribe(this.getKey(wf.name, C.QUEUE_DELAYED));
			}
		}
	}

	createCommandClient() {
		return new Promise(resolve => {
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
		return new Promise(resolve => {
			const client = this.createRedisClient();

			client.on("ready", async () => {
				this.subClient = client;
				this.connected = true;

				await client.subscribe(this.getKey(C.QUEUE_CATEGORY_SIGNAL));

				resolve(client);
				//this.logger.info("Workflows Redis adapter connected.");
			});
			client.on("error", err => {
				this.connected = false;
				this.logger.error("Workflows Redis adapter error", err.message);
			});
			client.on("end", () => {
				this.connected = false;
				this.subClient = null;
				//this.logger.info("Workflows Redis adapter disconnected.");
			});
			client.on("messageBuffer", (channelBuf, message) => {
				const channel = channelBuf.toString();
				if (channel === this.getKey(C.QUEUE_CATEGORY_SIGNAL)) {
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
							storePromise.reject(this.broker.errorRegenerator.restore(json.error));
						} else {
							storePromise.resolve(json.result);
						}
					}
				} else if (channel.startsWith(this.prefix) && channel.endsWith(C.QUEUE_DELAYED)) {
					const json = this.serializer.deserialize(message);
					const wf = this.workflows.get(json.workflow);
					if (wf) {
						this.setNextDelayedMaintenance(wf, json.promoteAt);
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

		try {
			await super.destroy();

			if (this.commandClient) {
				await this.closeClient(this.commandClient);
				this.commandClient = null;
			}

			if (this.subClient) {
				await this.closeClient(this.subClient);
				this.subClient = null;
			}

			for (const client of this.jobClients.values()) {
				if (client) {
					await this.closeClient(client);
				}
			}
		} catch (err) {
			this.logger.warn("Error while disconnecting from Redis", err.message);
		}
		this.jobClients.clear();

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
			let client = this.jobClients.get(workflowName);
			if (client && client.status === "ready") {
				resolve(client);
			} else {
				if (!client) {
					client = this.createRedisClient();
					this.jobClients.set(workflowName, client);
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
	 * Close a Redis client.
	 * @param {Redis} client
	 */
	async closeClient(client) {
		if (client) {
			try {
				if (client.blocked) {
					await client.disconnect();
				} else {
					await client.quit();
				}
			} catch (err) {
				if (err.message !== "Connection is closed.") {
					this.log("warn", null, null, "Error while closing Redis client", err.message);
				}
			}
		}
	}

	/**
	 * Start the job processor for the given workflow.
	 *
	 * @param {string} workflow - The name of the workflow.
	 */
	startJobProcessor(workflow) {
		if (!this.jobClients.has(workflow.name)) {
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
		if (this.jobClients.has(workflow.name)) {
			const client = this.jobClients.get(workflow.name);
			await this.closeClient(client);
			client.stopped = true;
		}
	}

	/**
	 * Job processor for the given workflow. Waits for a job in the waiting queue, moves it to the active queue, and processes it.
	 *
	 * @param {string} workflow - The name of the workflow.
	 * @returns {Promise<void>} Resolves when the job is processed.
	 */
	async runJobProcessor(workflow) {
		// if (workflow.$isRunning) {
		// 	return;
		// }

		if (this.disconnecting || !this.connected) {
			this.log("debug", workflow.name, null, "Adapter is not connected or disconnecting.");
			return;
		}

		// workflow.$isRunning = true;
		const client = await this.isClientReady(workflow.name);
		if (client.stopped) {
			this.log("debug", workflow.name, null, "Job processor is stopped");
			return;
		}

		const concurrency = workflow.concurrency ?? 1;
		const runningJobs = this.activeRuns.get(workflow.name) || [];

		if (runningJobs.length >= concurrency) {
			// this.log("debug", workflow.name, null, "Max concurrency reached. Waiting...");
			//workflow.$isRunning = false;
			//setTimeout(() => this.runJobProcessor(workflow), 500);
			setImmediate(() => this.runJobProcessor(workflow));
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
				this.addRunningJob(workflow, jobId);
				// Process the job, without awaiting for it
				this.processJob(workflow, jobId);
			}
		} catch (err) {
			// Swallow error if disconnecting
			if (!this.disconnecting && err.message != "Connection is closed.") {
				this.log("error", workflow.name, null, "Unable to watch job", err);
			}
		} finally {
			setImmediate(() => this.runJobProcessor(workflow));
		}
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

			this.removeRunningJob(workflow, jobId);

			return;
		}

		const jobEvents = await this.getJobEvents(workflow.name, jobId);

		if (!job.startedAt) {
			job.startedAt = Date.now();
		}

		let unlock;
		try {
			unlock = await this.lock(workflow, jobId);

			await this.commandClient.hsetnx(
				this.getKey(workflow.name, C.QUEUE_JOB, jobId),
				"startedAt",
				job.startedAt
			);

			await this.addJobEvent(workflow.name, job.id, {
				type: "started"
			});

			this.sendJobEvent(workflow.name, job.id, "started");

			this.log("debug", workflow.name, jobId, "Running job...", job);

			const result = await this.callWorkflowHandler(workflow, job, jobEvents);

			const duration = Date.now() - job.startedAt;
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
			this.removeRunningJob(workflow, jobId);

			if (unlock) await unlock();

			//setImmediate(() => this.runJobProcessor(workflow));
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
			"EX",
			this.mwOpts.lockExpiration,
			"NX"
		);
		this.log("debug", workflow.name, jobId, "Lock result", lockRes);

		if (!lockRes) throw new WorkflowError(`Job ${jobId} is already locked.`);

		const lockExtender = async () => {
			if (this.disconnecting || !this.connected) {
				clearInterval(timer);
				return;
			}

			// Check if the job is not finished (e.g timeout maintainer process is closed)
			const finishedAt = await this.commandClient.hget(
				this.getKey(workflow.name, C.QUEUE_JOB, jobId),
				"finishedAt"
			);

			if (finishedAt > 0) {
				this.log("debug", workflow.name, jobId, "Job is finished. Unlocking...");
				clearInterval(timer);
				await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId));
				return;
			}

			// Extend the lock
			this.log("debug", workflow.name, jobId, "Extending lock");
			const lockRes = await this.commandClient.set(
				this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId),
				this.broker.instanceID,
				"EX",
				this.mwOpts.lockExpiration,
				"XX"
			);
			if (!lockRes) {
				this.log("debug", workflow.name, jobId, "Job lock is expired");
				return;
			}
		};

		// Start lock extender
		const timer = setInterval(() => lockExtender(), (this.mwOpts.lockExpiration / 2) * 1000);

		return async () => {
			clearInterval(timer);
			if (this.disconnecting || !this.connected) return;

			const unlockRes = await this.commandClient.del(
				this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId)
			);
			this.log("debug", workflow.name, jobId, "Unlock result", unlockRes);
		};
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
			fields = ["payload", "parent", "startedAt", "retries", "retryAttempts", "timeout"];
		}

		const exists = await this.commandClient.exists(
			this.getKey(workflowName, C.QUEUE_JOB, jobId)
		);
		if (!exists) return null;

		let job;

		if (fields === true) {
			job = await this.commandClient.hgetallBuffer(
				this.getKey(workflowName, C.QUEUE_JOB, jobId)
			);
		} else {
			const values = await this.commandClient.hmgetBuffer(
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
		const jobEvents = await this.commandClient.lrangeBuffer(
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
		if (!this.connected || this.disconnecting) return;

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
		if (!this.connected || this.disconnecting) return;

		await this.addJobEvent(workflow.name, job.id, {
			type: "finished"
		});

		this.sendJobEvent(workflow.name, job.id, "finished");
		this.sendJobEvent(workflow.name, job.id, "completed");

		const pipeline = this.commandClient.pipeline();
		pipeline.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, job.id);
		pipeline.srem(this.getKey(workflow.name, C.QUEUE_STALLED), job.id);

		if (workflow.removeOnCompleted) {
			pipeline.del(this.getKey(workflow.name, C.QUEUE_JOB, job.id));
		} else {
			const fields = {
				success: true,
				finishedAt: Date.now(),
				nodeID: this.broker.nodeID,
				duration: Date.now() - job.startedAt
			};
			if (result != null) {
				fields.result = this.serializer.serialize(result);
			}
			pipeline.hmset(this.getKey(workflow.name, C.QUEUE_JOB, job.id), fields);
			pipeline.zadd(this.getKey(workflow.name, C.QUEUE_COMPLETED), fields.finishedAt, job.id);
		}

		await pipeline.exec();

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

	/**
	 * Calculate the backoff time for a job.
	 *
	 * @param {Workflow} workflow
	 * @param {number} retryAttempts
	 * @returns {number} The backoff time in milliseconds.
	 */
	getBackoffTime(workflow, retryAttempts) {
		const backoff = workflow.backoff || "fixed";
		const delay = parseDuration(workflow.backoffDelay) ?? 100;
		if (typeof backoff === "function") {
			return backoff(retryAttempts);
		} else if (backoff === "fixed") {
			return delay;
		} else if (backoff === "exponential") {
			return Math.pow(2, retryAttempts) * delay;
		}

		return delay;
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
		if (!this.connected || this.disconnecting) return;

		if (typeof job == "string") {
			job = await this.getJob(workflow.name, job, ["parent", "startedAt"]);
		}

		await this.addJobEvent(workflow.name, job.id, {
			type: "failed",
			error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
		});

		this.sendJobEvent(workflow.name, job.id, "finished");
		this.sendJobEvent(workflow.name, job.id, "failed");

		const pipeline = this.commandClient.pipeline();
		pipeline.lrem(this.getKey(workflow.name, C.QUEUE_ACTIVE), 1, job.id);
		pipeline.srem(this.getKey(workflow.name, C.QUEUE_STALLED), job.id);
		await pipeline.exec();

		if (err?.retryable) {
			let retryFields = await this.commandClient.hmget(
				this.getKey(workflow.name, C.QUEUE_JOB, job.id),
				["retries", "retryAttempts"]
			);
			const retries = parseInt(retryFields[0] ?? 0);
			const retryAttempts = parseInt(retryFields[1] ?? 0);

			if (retries > 0 && retryAttempts < retries) {
				const pipeline = this.commandClient.pipeline();
				pipeline.hincrby(
					this.getKey(workflow.name, C.QUEUE_JOB, job.id),
					"retryAttempts",
					1
				);
				const backoffTime = this.getBackoffTime(workflow, retryAttempts);
				this.log(
					"debug",
					workflow.name,
					job.id,
					`Retrying job (${retryAttempts} attempts of ${retries}) after ${backoffTime} ms...`
				);

				const promoteAt = Date.now() + backoffTime;
				pipeline.hset(
					this.getKey(workflow.name, C.QUEUE_JOB, job.id),
					"promoteAt",
					promoteAt
				);
				pipeline.zadd(this.getKey(workflow.name, C.QUEUE_DELAYED), promoteAt, job.id);

				await pipeline.exec();

				// Publish promoteAt time to all workers
				await this.sendDelayedJobPromoteAt(workflow.name, job.id, promoteAt);

				return;
			}
		}

		if (workflow.removeOnFailed) {
			await this.commandClient.del(this.getKey(workflow.name, C.QUEUE_JOB, job.id));
		} else {
			const fields = {
				success: false,
				finishedAt: Date.now(),
				nodeID: this.broker.nodeID,
				duration: Date.now() - job.startedAt
			};
			if (err != null) {
				fields.error = this.serializer.serialize(
					this.broker.errorRegenerator.extractPlainError(err)
				);
			}

			this.log("debug", workflow.name, job.id, `Job move to failed queue.`);

			const pipeline = this.commandClient.pipeline();
			pipeline.hmset(this.getKey(workflow.name, C.QUEUE_JOB, job.id), fields);
			pipeline.zadd(this.getKey(workflow.name, C.QUEUE_FAILED), fields.finishedAt, job.id);
			await pipeline.exec();
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
		const state = await this.commandClient.hgetBuffer(
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
		if (key == null) key = SIGNAL_EMPTY_KEY;
		const content = this.serializer.serialize({
			signal: signalName,
			key,
			payload
		});

		this.log("debug", signalName, key, "Trigger signal", payload);

		const exp = parseDuration(this.mwOpts.signalExpiration);
		if (exp != null && exp > 0) {
			await this.commandClient.set(this.getSignalKey(signalName, key), content, "PX", exp);
		} else {
			await this.commandClient.set(this.getSignalKey(signalName, key), content);
		}

		await this.commandClient.publish(this.getKey(C.QUEUE_CATEGORY_SIGNAL), content);
	}

	/**
	 * Remove a named signal.
	 *
	 * @param {string} signalName - The name of the signal.
	 * @param {unknown} key - The key associated with the signal.
	 * @returns {Promise<void>} Resolves when the signal is triggered.
	 */
	async removeSignal(signalName, key) {
		if (key == null) key = SIGNAL_EMPTY_KEY;
		this.log("debug", signalName, key, "Remove signal", signalName, key);

		await this.commandClient.del(this.getSignalKey(signalName, key));
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param {string} signalName - The name of the signal.
	 * @param {unknown} key - The key associated with the signal.
	 * @returns {Promise<unknown>} Resolves with the signal payload.
	 */
	async waitForSignal(signalName, key) {
		if (key == null) key = SIGNAL_EMPTY_KEY;
		const content = await this.commandClient.getBuffer(this.getSignalKey(signalName, key));
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

		let customJobId = !!opts.jobId;

		let job = {
			id: opts.jobId ? this.checkJobId(opts.jobId) : this.broker.generateUid(),
			createdAt: Date.now()
		};

		let isLoadedJob = false;

		if (customJobId) {
			// Job ID collision check
			const exists = await this.commandClient.exists(
				this.getKey(workflowName, C.QUEUE_JOB, job.id)
			);
			if (exists) {
				if (this.mwOpts.jobIdCollision == "reject") {
					throw new WorkflowError(
						`Job ID '${job.id}' already exists.`,
						400,
						"JOB_ID_EXISTS"
					);
				} else if (this.mwOpts.jobIdCollision == "skip") {
					job = await this.getJob(workflowName, job.id, true);
					isLoadedJob = true;
				} else if (this.mwOpts.jobIdCollision == "rerun") {
					const foundJob = await this.getJob(workflowName, job.id, true);
					if (foundJob.finishedAt) {
						job.createdAt = foundJob.createdAt;

						// Remove previous job data
						await this.commandClient.hdel(
							this.getKey(workflowName, C.QUEUE_JOB, job.id),
							[
								"startedAt",
								"duration",
								"finishedAt",
								"nodeID",
								"success",
								"error",
								"result"
							]
						);
					} else {
						isLoadedJob = true;
						job = foundJob;
					}
				}
			}
		}

		if (!isLoadedJob) {
			if (payload != null) {
				job.payload = payload;
			}

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
							"Repeatable job is expired at " +
								new Date(opts.repeat.endDate).toISOString(),
							400,
							"REPEAT_JOB_EXPIRED",
							{
								jobId: job.id,
								endDate: opts.repeat.endDate
							}
						);
					}
				}
			}

			// Save the Job to Redis
			await this.commandClient.hmset(
				this.getKey(workflowName, C.QUEUE_JOB, job.id),
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
					job.id
				);

				// Publish promoteAt time to all workers
				await this.sendDelayedJobPromoteAt(workflowName, job.id, job.promoteAt);
			} else {
				// Normal job
				this.log("debug", workflowName, job.id, "Job created.", job);
				await this.commandClient.lpush(this.getKey(workflowName, C.QUEUE_WAITING), job.id);
			}

			this.sendJobEvent(workflowName, job.id, "created");
		}

		job.promise = async () => {
			// Subscribe to the job finished event
			await this.subClient?.subscribe(this.getKey(workflowName, C.FINISHED));

			// Get the Job to check the status
			const job2 = await this.getJob(workflowName, job.id, [
				"success",
				"finishedAt",
				"error",
				"result"
			]);

			if (!job2) {
				throw new WorkflowError("Job not found", 404, "JOB_NOT_FOUND", { jobId: job.id });
			}

			// If the job is finished, return the result or error
			if (job2.finishedAt) {
				if (job2.success) return job2.result;
				throw this.broker.errorRegenerator.restore(job2.error);
			}

			// Check that Job promise is stored
			if (this.jobResultPromises.has(job.id)) {
				return this.jobResultPromises.get(job.id).promise;
			}

			// Store Job finished promise
			const storePromise = {};
			storePromise.promise = new Promise((resolve, reject) => {
				storePromise.resolve = resolve;
				storePromise.reject = reject;
			});

			this.jobResultPromises.set(job.id, storePromise);

			return storePromise.promise;
		};

		return job;
	}

	/**
	 * Send a delayed job promoteAt message to all workers.
	 *
	 * @param {string} workflowName
	 * @param {string} jobId
	 * @param {number} promoteAt
	 */
	async sendDelayedJobPromoteAt(workflowName, jobId, promoteAt) {
		// Check if this job is at the head of the delayed queue
		const head = await this.commandClient.zrange(
			this.getKey(workflowName, C.QUEUE_DELAYED),
			0,
			0
		);
		if (head && head[0] === jobId) {
			this.log(
				"debug",
				workflowName,
				jobId,
				"Publish delayed job promote time",
				new Date(promoteAt).toISOString()
			);
			const msg = this.serializer.serialize({
				workflow: workflowName,
				promoteAt
			});
			await this.commandClient.publish(this.getKey(workflowName, C.QUEUE_DELAYED), msg);
		}
	}

	/**
	 * Set the next delayed jobs maintenance timer for a workflow.
	 * @param {Object} wf - The workflow object.
	 * @param {number} [nextTime] - Optional timestamp to schedule next maintenance.
	 */
	async setNextDelayedMaintenance(wf, nextTime) {
		if (nextTime == null) {
			const first = await this.commandClient?.zrange(
				this.getKey(wf.name, C.QUEUE_DELAYED),
				0,
				0,
				"WITHSCORES"
			);

			if (first?.length > 0) {
				const promoteAt = parseInt(first[1]);
				if (promoteAt > Date.now()) {
					super.setNextDelayedMaintenance(wf, promoteAt);
					return;
				}
			}
		}

		super.setNextDelayedMaintenance(wf, nextTime);
	}

	/**
	 * Serialize a job object for storage in Redis.
	 *
	 * @param {Object} job - The job object to serialize.
	 * @returns {Object} The serialized job object.
	 */
	serializeJob(job) {
		const res = { ...job };

		for (const field of JOB_FIELDS_JSON) {
			if (job[field] != null) {
				res[field] = this.serializer.serialize(job[field]);
			}
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
		const res = {};

		for (const [key, value] of Object.entries(job)) {
			if (JOB_FIELDS_JSON.includes(key)) {
				if (value != null) {
					res[key] = value !== "" ? this.serializer.deserialize(value) : null;
				}
			} else if (JOB_FIELDS_NUMERIC.includes(key)) {
				if (value != null) {
					res[key] = value !== "" ? Number(value) : null;
				}
			} else if (JOB_FIELDS_BOOLEAN.includes(key)) {
				if (value != null) {
					res[key] = String(value) === "true";
				}
			} else {
				if (value != null) {
					res[key] = value !== "" ? String(value) : null;
				}
			}
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
					"startedAt",
					"timeout"
				]);

				if (!job) {
					this.log("warn", workflowName, job, "Parent job not found. Not rescheduling.");
					return;
				}
			}

			const nextJob = { ...job };
			delete nextJob.repeat;
			nextJob.createdAt = Date.now();
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

						await this.commandClient.hmset(
							this.getKey(workflowName, C.QUEUE_JOB, job.id),
							{
								finishedAt: Date.now(),
								nodeID: this.broker.nodeID
							}
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

						await this.commandClient.hmset(
							this.getKey(workflowName, C.QUEUE_JOB, job.id),
							{
								finishedAt: Date.now(),
								nodeID: this.broker.nodeID
							}
						);

						return;
					}
				}

				nextJob.repeatCounter = await this.commandClient.hincrby(
					this.getKey(workflowName, C.QUEUE_JOB, job.id),
					"repeatCounter",
					1
				);

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

				await this.sendDelayedJobPromoteAt(workflowName, nextJob.id, nextJob.promoteAt);
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
			const pipeline = this.commandClient.pipeline();
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB, jobId));
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB_LOCK, jobId));
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId));
			pipeline.lrem(this.getKey(workflowName, C.QUEUE_WAITING), 1, jobId);
			await pipeline.exec();
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
	 * @param {number} lockTime - The time to hold the lock in milliseconds.
	 * @returns {Promise<boolean>} Resolves with true if the lock is acquired, false otherwise.
	 */
	async lockMaintenance(workflow, lockTime, lockName = C.QUEUE_MAINTENANCE_LOCK) {
		// Set lock
		const lockRes = await this.commandClient?.set(
			this.getKey(workflow.name, lockName),
			this.broker.instanceID,
			"PX",
			lockTime / 2,
			"NX"
		);

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
				try {
					const pipeline = this.commandClient.pipeline();
					for (const jobId of jobIds) {
						const job = await this.getJob(workflow.name, jobId, ["id"]);
						if (job) {
							this.log(
								"debug",
								workflow.name,
								jobId,
								"Moving delayed job to waiting queue."
							);
							pipeline.lpush(this.getKey(workflow.name, C.QUEUE_WAITING), jobId);
						}
						pipeline.zrem(this.getKey(workflow.name, C.QUEUE_DELAYED), jobId);
					}
					await pipeline.exec();
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
	 * Check active jobs and if they timed out, move to failed jobs.
	 *
	 * @param {Object} workflow - The workflow object.
	 * @returns {Promise<void>} Resolves when delayed jobs are processed.
	 */
	async maintenanceActiveJobs(workflow) {
		this.log("debug", workflow.name, null, "Maintenance active jobs...");
		try {
			const now = Date.now();
			const activeJobIds = await this.commandClient.lrange(
				this.getKey(workflow.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				for (const jobId of activeJobIds) {
					try {
						const job = await this.getJob(workflow.name, jobId, [
							"startedAt",
							"timeout"
						]);
						if (job && job.startedAt > 0) {
							const timeout = parseDuration(
								job.timeout != null ? job.timeout : workflow.timeout
							);
							if (timeout > 0) {
								if (now - job.startedAt > timeout) {
									this.log(
										"debug",
										workflow.name,
										jobId,
										`Job timed out (${humanize(timeout)}). Moving to failed queue.`
									);
									await this.moveToFailed(
										workflow,
										jobId,
										new WorkflowTimeoutError(workflow.name, jobId, timeout)
									);
									// Unlock manually
									await this.commandClient.del(
										this.getKey(workflow.name, C.QUEUE_JOB_LOCK, jobId)
									);

									this.removeRunningJob(workflow, jobId);
								}
							}
						}
					} catch (err) {
						this.log(
							"error",
							workflow.name,
							jobId,
							"Error while timeout active job",
							err
						);
					}
				}
			}
		} catch (err) {
			this.log("error", workflow.name, null, "Error while processing active jobs", err);
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
	 * @param {number} retention - The age threshold in milliseconds for removing jobs.
	 * @returns {Promise<void>} Resolves when old jobs are removed.
	 */
	async maintenanceRemoveOldJobs(workflow, queueName, retention) {
		this.log(
			"debug",
			workflow.name,
			null,
			`Maintenance remove old ${queueName} jobs... (retention: ${humanize(retention)})`
		);

		try {
			const minDate = Date.now() - retention;
			const jobIds = await this.commandClient.zrangebyscore(
				this.getKey(workflow.name, queueName),
				0,
				minDate
			);
			if (jobIds.length > 0) {
				const jobKeys = jobIds.map(jobId => this.getKey(workflow.name, C.QUEUE_JOB, jobId));
				const jobEventsKeys = jobIds.map(jobId =>
					this.getKey(workflow.name, C.QUEUE_JOB_EVENTS, jobId)
				);
				const pipeline = this.commandClient.pipeline();
				pipeline.del(jobKeys);
				pipeline.del(jobEventsKeys);
				pipeline.zremrangebyscore(this.getKey(workflow.name, queueName), 0, minDate);
				await pipeline.exec();

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
	async unlockMaintenance(workflow, lockName = C.QUEUE_MAINTENANCE_LOCK) {
		const unlockRes = await this.commandClient?.del(this.getKey(workflow.name, lockName));
		this.log("debug", workflow.name, null, "Unlock maintenance", lockName, unlockRes);
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
	 * Get Redis key (for Workflow) for the given name and type.
	 *
	 * @param {string} name - The name of the workflow or entity.
	 * @param {string} type - The type of the key (e.g., job, lock, events).
	 * @param {string} [id] - Optional ID to append to the key.
	 * @returns {string} The constructed Redis key.
	 */
	getKey(name, type, id) {
		if (id) {
			return `${this.prefix}${C.QUEUE_CATEGORY_WF}:${name}:${type}:${id}`;
		} else if (type) {
			return `${this.prefix}${C.QUEUE_CATEGORY_WF}:${name}:${type}`;
		} else {
			return `${this.prefix}${C.QUEUE_CATEGORY_WF}:${name}`;
		}
	}

	/**
	 * Get Redis key for a signal.
	 *
	 * @param {*} signalName  The name of the signal
	 * @param {*} key The key of the signal
	 * @returns {string} The constructed Redis key for the signal.
	 */
	getSignalKey(signalName, key) {
		return `${this.prefix}${C.QUEUE_CATEGORY_SIGNAL}:${signalName}:${key}`;
	}

	/**
	 * Format the result of zrange command to an array of objects.
	 *
	 * @param {*} list
	 * @param {*} timeField
	 * @returns
	 */
	formatZrangeResultToObject(list, timeField = "finishedAt") {
		const arr = [];
		for (let i = 0; i < list.length; i += 2) {
			arr.push({
				id: list[i],
				[timeField]: Number(list[i + 1])
			});
		}

		return arr;
	}

	/**
	 * List all completed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<any>}
	 */
	async listCompletedJobs(workflowName) {
		return this.formatZrangeResultToObject(
			await this.commandClient.zrange(
				this.getKey(workflowName, C.QUEUE_COMPLETED),
				0,
				-1,
				"WITHSCORES"
			)
		);
	}

	/**
	 * List all failed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<any>}
	 */
	async listFailedJobs(workflowName) {
		return this.formatZrangeResultToObject(
			await this.commandClient.zrange(
				this.getKey(workflowName, C.QUEUE_FAILED),
				0,
				-1,
				"WITHSCORES"
			)
		);
	}

	/**
	 * List all delayed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<any>}
	 */
	async listDelayedJobs(workflowName) {
		return this.formatZrangeResultToObject(
			await this.commandClient.zrange(
				this.getKey(workflowName, C.QUEUE_DELAYED),
				0,
				-1,
				"WITHSCORES"
			),
			"promoteAt"
		);
	}

	/**
	 * List all active job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<any>}
	 */
	async listActiveJobs(workflowName) {
		return (
			await this.commandClient.lrange(this.getKey(workflowName, C.QUEUE_ACTIVE), 0, -1)
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * List all waiting job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<any>}
	 */
	async listWaitingJobs(workflowName) {
		return (
			await this.commandClient.lrange(this.getKey(workflowName, C.QUEUE_WAITING), 0, -1)
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * Dump all Redis data for all workflows to JSON files.
	 */
	async dumpWorkflows(folder) {
		makeDirs(folder);
		for (const name of this.workflows.keys()) {
			await this.dumpWorkflow(name, folder);
		}
	}

	/**
	 * Dump all Redis data for a workflow to a JSON file.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @returns {Promise<string>} Path to the dump file.
	 */
	async dumpWorkflow(workflowName, folder = ".") {
		const fs = require("fs").promises;
		const path = require("path");

		const pattern = this.getKey(workflowName) + ":*";
		const client = this.commandClient;
		let cursor = "0";
		let keys = [];
		// Scan all keys matching the workflow pattern
		do {
			const res = await client.scan(cursor, "MATCH", pattern, "COUNT", 100);
			cursor = res[0];
			keys.push(...res[1]);
		} while (cursor !== "0");

		const deserializeData = value => {
			if (_.isString(value) || Buffer.isBuffer(value)) {
				return this.serializer.deserialize(value);
			} else if (_.isObject(value)) {
				return this.deserializeJob(value);
			}

			return value;
		};

		const dump = {};
		for (const key of keys) {
			const type = await client.type(key);
			if (type === "list") {
				const list = await client.lrangeBuffer(key, 0, -1);
				dump[key] = key.includes(":" + C.QUEUE_JOB_EVENTS + ":")
					? list.map(item => deserializeData(item))
					: list.map(item => item);
			} else if (type === "set") {
				dump[key] = await client.smembers(key);
			} else if (type === "hash") {
				dump[key] = deserializeData(await client.hgetallBuffer(key));
			} else if (type === "zset") {
				const members = await client.zrange(key, 0, -1, "WITHSCORES");
				// Convert flat array to [{member, score}]
				const arr = [];
				for (let i = 0; i < members.length; i += 2) {
					arr.push({ member: members[i], score: Number(members[i + 1]) });
				}
				dump[key] = arr;
			} else if (type === "string") {
				dump[key] = deserializeData(await client.getBuffer(key));
			} else {
				dump[key] = null;
			}
		}

		const fileName = path.join(folder, `dump-${workflowName}.json`);
		await fs.writeFile(fileName, JSON.stringify(dump, null, 2), "utf8");
		this.logger.info(`Workflow '${workflowName}' dumped to ${fileName}`);
		return fileName;
	}
}

module.exports = RedisAdapter;
