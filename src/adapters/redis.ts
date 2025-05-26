/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

import _ from "lodash";
import Redis, { Cluster, ClusterOptions, Redis as RedisClient, RedisOptions } from "ioredis";
import { Serializers, ServiceBroker, LoggerInstance, Serializer } from "moleculer";
import { makeDirs } from "moleculer";
import BaseAdapter from "./base";
import {
	WorkflowError,
	WorkflowAlreadyLocked,
	WorkflowTimeoutError,
	WorkflowMaximumStalled
} from "../errors";
import * as C from "../constants";
import { parseDuration, humanize } from "../utils";
import Workflow from "../workflow";
import type { BaseDefaultOptions } from "./base";
import { CreateJobOptions, Job, JobEvent, WorkflowsMiddlewareOptions } from "../types";

export interface RedisAdapterOptions extends BaseDefaultOptions {
	redis?:
		| RedisOptions
		| { url: string }
		| { cluster: { nodes: string[]; clusterOptions?: ClusterOptions } };
	prefix?: string;
	serializer?: string;
	drainDelay?: number;
}

/**
 * Redis Adapter for Workflows
 */
export default class RedisAdapter extends BaseAdapter {
	declare opts: RedisAdapterOptions;
	public isWorker: boolean;
	public commandClient: RedisClient;
	public blockerClient: RedisClient;
	public subClient: RedisClient;
	public signalPromises: Map<
		string,
		{ promise: Promise<unknown>; resolve: (v: unknown) => void }
	>;
	public jobResultPromises: Map<
		string,
		{ promise: Promise<unknown>; resolve: (v: unknown) => void; reject: (e: unknown) => void }
	>;

	public running: boolean;
	public disconnecting: boolean;
	public prefix!: string;
	public serializer!: Serializer;
	declare wf: Workflow;
	declare broker: ServiceBroker;
	declare logger: LoggerInstance;
	declare mwOpts: WorkflowsMiddlewareOptions;

	/**
	 * Constructor of adapter.
	 */
	constructor(opts: RedisAdapterOptions) {
		if (typeof opts == "string")
			opts = {
				redis: {
					url: opts
				}
			};

		if (opts && typeof opts.redis == "string") {
			opts = {
				...opts,
				redis: {
					url: opts.redis
				}
			};
		}

		super(opts);

		this.opts = _.defaultsDeep(this.opts, {
			serializer: "JSON",
			drainDelay: 5,
			redis: {
				retryStrategy: times => Math.min(times * 500, 5000)
			}
		});

		this.isWorker = false;

		this.commandClient = null;
		this.blockerClient = null;
		this.subClient = null;

		this.signalPromises = new Map();
		this.jobResultPromises = new Map();

		this.running = false;
		this.disconnecting = false;

		// Workflow = require("../workflow");
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param wf
	 * @param broker
	 * @param logger
	 * @param mwOpts - Middleware options.
	 */
	init(
		wf: Workflow | null,
		broker: ServiceBroker,
		logger: LoggerInstance,
		mwOpts: WorkflowsMiddlewareOptions
	) {
		super.init(wf, broker, logger, mwOpts);

		this.isWorker = !!wf;

		if (this.opts.prefix) {
			this.prefix = this.opts.prefix + ":";
		} else if (this.broker.namespace) {
			this.prefix = "molwf-" + this.broker.namespace + ":";
		} else {
			this.prefix = "molwf:";
		}

		this.logger.debug("Workflows Redis adapter prefix:", this.prefix);

		// create an instance of serializer (default to JSON)
		/** @type {Serializer} */
		this.serializer = Serializers.resolve(this.opts.serializer);
		this.serializer.init(this.broker);
		this.logger.info("Workflows serializer:", this.broker.getConstructorName(this.serializer));
	}

	/**
	 * Connect to Redis.
	 *
	 * @returns Resolves when the connection is established.
	 */
	async connect(): Promise<void> {
		if (this.connected) return;

		await this.createCommandClient();
		await this.createSubscriptionClient();
		if (this.isWorker) {
			await this.createBlockerClient();
		}
	}

	/**
	 * Create a Redis client.
	 *
	 * @param name Connection name
	 * @returns
	 */
	async createRedisClient(name: string): Promise<RedisClient> {
		return new Promise(resolve => {
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
				client = new Redis.Command(opts && opts.url ? opts.url : opts);
			}

			client.on("ready", () => {
				this.connected = true;
				resolve(client);
				this.log("info", this.wf?.name ?? "", null, `Redis adapter (${name}) connected.`);
			});
			client.on("error", err => {
				this.connected = false;
				this.log(
					"info",
					this.wf?.name ?? "",
					null,
					`Redis adapter (${name}) error`,
					err.message
				);
			});
			client.on("end", () => {
				this.connected = false;
				this.log(
					"info",
					this.wf?.name ?? "",
					null,
					`Redis adapter (${name}) disconnected.`
				);
			});
		});
	}

	async createCommandClient(): Promise<void> {
		this.commandClient = await this.createRedisClient("command");
	}

	async createBlockerClient() {
		this.blockerClient = await this.createRedisClient("blocker");
	}

	async createSubscriptionClient() {
		this.subClient = await this.createRedisClient("subscription");
		this.subClient.on("messageBuffer", (channelBuf, message) => {
			const channel = channelBuf.toString();
			if (channel.startsWith(this.getKey(C.QUEUE_CATEGORY_SIGNAL + ":"))) {
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
						storePromise.reject(this.broker.errorRegenerator.restore(json.error, {}));
					} else {
						storePromise.resolve(json.result);
					}
				}
			}
			if (this.isWorker) {
				if (channel == this.getKey(this.wf.name, C.QUEUE_DELAYED)) {
					const json = this.serializer.deserialize(message);
					this.wf.setNextDelayedMaintenance(json.promoteAt);
				}
			}
		});

		if (this.isWorker) {
			await this.subClient.subscribe(this.getKey(this.wf.name, C.QUEUE_DELAYED));
		}
	}

	/**
	 * Close the adapter connection
	 *
	 * @returns Resolves when the disconnection is complete.
	 */
	async disconnect(): Promise<void> {
		if (this.disconnecting) return;

		this.disconnecting = true;
		this.connected = false;

		// await super.disconnect();

		if (this.commandClient) {
			await this.closeClient(this.commandClient);
			this.commandClient = null;
		}

		if (this.subClient) {
			await this.closeClient(this.subClient);
			this.subClient = null;
		}

		if (this.blockerClient) {
			await this.closeClient(this.blockerClient);
			this.blockerClient = null;
		}

		this.disconnecting = false;
	}

	/**
	 * Wait for Redis client to be ready. If it does not exist, it will create a new one.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @returns {Promise<Redis>} Resolves with the Redis client instance.
	 *
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
	}*/

	/**
	 * Close a Redis client.
	 *
	 * @param client
	 */
	async closeClient(client: RedisClient): Promise<void> {
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
	 */
	startJobProcessor() {
		this.running = true;
		this.runJobProcessor();
	}

	/**
	 * Stop the job processor for the given workflow.
	 */
	stopJobProcessor() {
		this.running = false;
	}

	/**
	 * Job processor for the given workflow. Waits for a job in the waiting queue, moves it to the active queue, and processes it.
	 *
	 * @returns Resolves when the job is processed.
	 */
	async runJobProcessor(): Promise<void> {
		if (!this.running || this.blockerClient.blocked) return;

		// if (this.disconnecting || !this.connected) {
		// 	this.log("debug", workflow.name, null, "Adapter is not connected or disconnecting.");
		// 	return;
		// }

		const concurrency = this.wf.opts.concurrency;
		const activeJobCount = this.wf.getNumberOfActiveJobs();

		if (activeJobCount >= concurrency) {
			// this.log("debug", workflow.name, null, "Max concurrency reached. Waiting...");
			return;
		}

		try {
			// console.log(" >>> BLOCK START >>>", activeJobCount);
			this.blockerClient.blocked = true;
			const jobId = await this.blockerClient.brpoplpush(
				this.getKey(this.wf.name, C.QUEUE_WAITING),
				this.getKey(this.wf.name, C.QUEUE_ACTIVE),
				this.opts.drainDelay
			);
			this.blockerClient.blocked = false;
			// console.log(" >>> BLOCK END >>>", this.wf.getNumberOfActiveJobs());

			if (jobId) {
				this.wf.addRunningJob(jobId);
				// Process the job, without awaiting for it
				this.processJob(jobId);
			}
		} catch (err) {
			// Swallow error if disconnecting
			if (!this.disconnecting && err.message != "Connection is closed.") {
				this.log("error", this.wf.name, null, "Unable to watch job", err);
			}
		} finally {
			setImmediate(() => this.runJobProcessor());
		}
	}

	/**
	 * Get job details and process it.
	 *
	 * @param jobId - The ID of the job to process.
	 * @returns Resolves when the job is processed.
	 */
	async processJob(jobId: string): Promise<void> {
		/** @type {Job} */
		const job = await this.getJob(this.wf.name, jobId);
		if (!job) {
			this.log("warn", this.wf.name, jobId, "Job not found");
			// Remove from active queue
			await this.commandClient.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, jobId);

			this.wf.removeRunningJob(jobId);

			return;
		}

		/** @type {JobEvent[]} */
		const jobEvents = await this.getJobEvents(this.wf.name, jobId);

		if (!job.startedAt) {
			job.startedAt = Date.now();
		}

		let unlock;
		try {
			unlock = await this.lock(jobId);

			await this.commandClient.hsetnx(
				this.getKey(this.wf.name, C.QUEUE_JOB, jobId),
				"startedAt",
				job.startedAt
			);

			await this.addJobEvent(this.wf.name, job.id, {
				type: "started"
			});

			this.sendJobEvent(this.wf.name, job.id, "started");

			this.log("debug", this.wf.name, jobId, "Running job...", job);

			const result = await this.wf.callHandler(job, jobEvents);

			const duration = Date.now() - job.startedAt;
			this.log(
				"debug",
				this.wf.name,
				jobId,
				`Job finished in ${humanize(duration)}.`,
				result
			);

			await this.moveToCompleted(job, result);
		} catch (err) {
			if (err instanceof WorkflowAlreadyLocked) {
				this.log("debug", this.wf.name, jobId, "Job is already locked.");
				return;
			}
			this.log("error", this.wf.name, jobId, "Job processing is failed.", err);
			await this.moveToFailed(job, err);
		} finally {
			this.wf.removeRunningJob(jobId);

			if (unlock) await unlock();

			setImmediate(() => this.runJobProcessor());
		}
	}

	/**
	 * Set a lock for the job.
	 *
	 * @param {string} jobId - The ID of the job to lock.
	 * @returns {Promise<Function>} Resolves with a function to unlock the job.
	 */
	async lock(jobId: string): Promise<() => Promise<void>> {
		// Set lock
		const lockRes = await this.commandClient.set(
			this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId),
			this.broker.instanceID,
			"EX",
			this.mwOpts.lockExpiration,
			"NX"
		);
		this.log("debug", this.wf.name, jobId, "Lock result", lockRes);

		if (!lockRes) throw new WorkflowError(`Job ${jobId} is already locked.`);

		const lockExtender = async () => {
			if (this.disconnecting || !this.connected) {
				clearInterval(timer);
				return;
			}

			// Check if the job is not finished (e.g timeout maintainer process is closed)
			const finishedAt = await this.commandClient.hget(
				this.getKey(this.wf.name, C.QUEUE_JOB, jobId),
				"finishedAt"
			);

			if (finishedAt > 0) {
				this.log("debug", this.wf.name, jobId, "Job is finished. Unlocking...");
				clearInterval(timer);
				await this.commandClient.del(this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId));
				return;
			}

			// Extend the lock
			this.log("debug", this.wf.name, jobId, "Extending lock");
			const lockRes = await this.commandClient.set(
				this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId),
				this.broker.instanceID,
				"EX",
				this.mwOpts.lockExpiration,
				"XX"
			);
			if (!lockRes) {
				this.log("debug", this.wf.name, jobId, "Job lock is expired");
				return;
			}
		};

		// Start lock extender
		const timer = setInterval(() => lockExtender(), (this.mwOpts.lockExpiration / 2) * 1000);

		return async () => {
			clearInterval(timer);
			if (this.disconnecting || !this.connected) return;

			const unlockRes = await this.commandClient.del(
				this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId)
			);
			this.log("debug", this.wf.name, jobId, "Unlock result", unlockRes);
		};
	}

	/**
	 * Get a job from Redis.
	 *
	 * @param workflowName - Workflow name
	 * @param jobId - The ID of the job.
	 * @param fields - The fields to retrieve or true to retrieve all fields.
	 * @returns Resolves with the job object or null if not found.
	 */
	async getJob(
		workflowName: string,
		jobId: string,
		fields: string[] | boolean = null
	): Promise<Job | null> {
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

			// @ts-ignore
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
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @returns Resolves with an array of job events.
	 */
	async getJobEvents(workflowName: string, jobId: string): Promise<JobEvent[]> {
		const jobEvents = await this.commandClient.lrangeBuffer(
			this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId),
			0,
			-1
		);

		return jobEvents.map(e => this.serializer.deserialize(e) as JobEvent);
	}

	/**
	 * Add a job event to Redis.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @param event - The event object to add.
	 * @returns Resolves when the event is added.
	 */
	async addJobEvent(
		workflowName: string,
		jobId: string,
		event: Partial<JobEvent>
	): Promise<void> {
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
	 * @param job - The job object.
	 * @param result - The result of the job execution.
	 * @returns Resolves when the job is moved to the completed queue.
	 */
	async moveToCompleted(job: Job, result: unknown): Promise<void> {
		if (!this.connected || this.disconnecting) return;

		await this.addJobEvent(this.wf.name, job.id, {
			type: "finished"
		});

		this.sendJobEvent(this.wf.name, job.id, "finished");
		this.sendJobEvent(this.wf.name, job.id, "completed");

		const pipeline = this.commandClient.pipeline();
		pipeline.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, job.id);
		pipeline.srem(this.getKey(this.wf.name, C.QUEUE_STALLED), job.id);

		if (this.wf.opts.removeOnCompleted) {
			pipeline.del(this.getKey(this.wf.name, C.QUEUE_JOB, job.id));
		} else {
			/** @type {Partial<Job>} */
			const fields = {
				success: true,
				finishedAt: Date.now(),
				nodeID: this.broker.nodeID,
				duration: Date.now() - job.startedAt
			};
			if (result != null) {
				fields.result = this.serializer.serialize(result);
			}
			pipeline.hmset(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), fields);
			pipeline.zadd(this.getKey(this.wf.name, C.QUEUE_COMPLETED), fields.finishedAt, job.id);
		}

		await pipeline.exec();

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.resolve(result);
		} else {
			this.commandClient.publish(
				this.getKey(this.wf.name, C.FINISHED),
				this.serializer.serialize({
					jobId: job.id,
					result
				})
			);
		}

		if (job.parent) {
			await Workflow.rescheduleJob(this, this.wf.name, job.parent);
		}
	}

	/**
	 * Calculate the backoff time for a job.
	 *
	 * @param retryAttempts
	 * @returns The backoff time in milliseconds.
	 */
	getBackoffTime(retryAttempts: number): number {
		const opts = this.wf.opts.retryPolicy || {};
		const delay = parseDuration(opts.delay) ?? 100;

		return Math.min(
			delay * Math.pow(opts.factor || 1, retryAttempts),
			opts.maxDelay ?? Number.POSITIVE_INFINITY
		);
	}

	/**
	 * Move a job to the failed queue.
	 *
	 * @param job - The job object.
	 * @param err - The error that caused the job to fail.
	 * @returns Resolves when the job is moved to the failed queue.
	 */
	async moveToFailed(job: Job | string, err: Error | null): Promise<void> {
		if (!this.connected || this.disconnecting) return;

		if (typeof job == "string") {
			job = await this.getJob(this.wf.name, job, ["parent", "startedAt"]);
		}

		await this.addJobEvent(this.wf.name, job.id, {
			type: "failed",
			error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
		});

		this.sendJobEvent(this.wf.name, job.id, "finished");
		this.sendJobEvent(this.wf.name, job.id, "failed");

		const pipeline = this.commandClient.pipeline();
		pipeline.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, job.id);
		pipeline.srem(this.getKey(this.wf.name, C.QUEUE_STALLED), job.id);
		await pipeline.exec();

		// @ts-ignore
		if (err?.retryable) {
			const retryFields = await this.commandClient.hmget(
				this.getKey(this.wf.name, C.QUEUE_JOB, job.id),
				["retries", "retryAttempts"]
			);
			const retries =
				(retryFields[0] != null
					? parseInt(retryFields[0])
					: this.wf.opts.retryPolicy?.retries) ?? 0;
			const retryAttempts = parseInt(retryFields[1] ?? 0);

			if (retries > 0 && retryAttempts < retries) {
				const pipeline = this.commandClient.pipeline();
				pipeline.hincrby(
					this.getKey(this.wf.name, C.QUEUE_JOB, job.id),
					"retryAttempts",
					1
				);
				const backoffTime = this.getBackoffTime(retryAttempts);
				this.log(
					"debug",
					this.wf.name,
					job.id,
					`Retrying job (${retryAttempts} attempts of ${retries}) after ${backoffTime} ms...`
				);

				const promoteAt = Date.now() + backoffTime;
				pipeline.hset(
					this.getKey(this.wf.name, C.QUEUE_JOB, job.id),
					"promoteAt",
					promoteAt
				);
				pipeline.zadd(this.getKey(this.wf.name, C.QUEUE_DELAYED), promoteAt, job.id);

				await pipeline.exec();

				// Publish promoteAt time to all workers
				await this.sendDelayedJobPromoteAt(this.wf.name, job.id, promoteAt);

				return;
			}
		}

		if (this.wf.opts.removeOnFailed) {
			await this.commandClient.del(this.getKey(this.wf.name, C.QUEUE_JOB, job.id));
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

			this.log("debug", this.wf.name, job.id, `Job move to failed queue.`);

			const pipeline = this.commandClient.pipeline();
			pipeline.hmset(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), fields);
			pipeline.zadd(this.getKey(this.wf.name, C.QUEUE_FAILED), fields.finishedAt, job.id);
			await pipeline.exec();
		}

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.reject(err);
		} else {
			this.commandClient.publish(
				this.getKey(this.wf.name, C.FINISHED),
				this.serializer.serialize({
					jobId: job.id,
					error: this.broker.errorRegenerator.extractPlainError(err)
				})
			);
		}

		if (job.parent) {
			await Workflow.rescheduleJob(this, this.wf.name, job.parent);
		}
	}

	/**
	 * Save the state of a job.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @param state - The state object to save.
	 * @returns Resolves when the state is saved.
	 */
	async saveJobState(workflowName: string, jobId: string, state: unknown): Promise<void> {
		await this.commandClient.hset(
			this.getKey(workflowName, C.QUEUE_JOB, jobId),
			"state",
			this.serializer.serialize(state)
		);
		this.log("debug", workflowName, jobId, "Job state set.", state);
	}

	/**
	 * Get state of a workflow run.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns Resolves with the state object or null if not found.
	 */
	async getState(workflowName: string, jobId: string): Promise<unknown> {
		const state = await this.commandClient.hgetBuffer(
			this.getKey(workflowName, C.QUEUE_JOB, jobId),
			"state"
		);

		return state != null ? this.serializer.deserialize(state) : null;
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param signalName - The name of the signal.
	 * @param key - The key associated with the signal.
	 * @param payload - The payload of the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	async triggerSignal(signalName: string, key?: string, payload?: unknown): Promise<void> {
		if (key == null) key = C.SIGNAL_EMPTY_KEY;
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

		await this.commandClient.publish(this.getKey(C.QUEUE_CATEGORY_SIGNAL, signalName), content);
	}

	/**
	 * Remove a named signal.
	 *
	 * @param signalName - The name of the signal.
	 * @param key - The key associated with the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	async removeSignal(signalName: string, key?: string): Promise<void> {
		if (key == null) key = C.SIGNAL_EMPTY_KEY;
		this.log("debug", signalName, key, "Remove signal", signalName, key);

		await this.commandClient.del(this.getSignalKey(signalName, key));
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param signalName - The name of the signal.
	 * @param key - The key associated with the signal.
	 * @returns Resolves with the signal payload.
	 */
	async waitForSignal(signalName: string, key?: string): Promise<unknown> {
		if (key == null) key = C.SIGNAL_EMPTY_KEY;
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

		await this.subClient.subscribe(this.getKey(C.QUEUE_CATEGORY_SIGNAL, signalName));

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
	 * @param {Job} job - The job.
	 * @param {Record<string, any>} opts - Additional options for the job.
	 * @returns {Promise<Job>} Resolves with the created job object.
	 */
	async newJob(workflowName: string, job: Job, opts: Partial<CreateJobOptions>) {
		opts = opts || {};

		let isLoadedJob = false;

		if (opts.isCustomJobId) {
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
							C.RERUN_REMOVABLE_FIELDS
						);
					} else {
						isLoadedJob = true;
						job = foundJob;
					}
				}
			}
		}

		if (!isLoadedJob) {
			// Save the Job to Redis
			await this.commandClient.hmset(
				this.getKey(workflowName, C.QUEUE_JOB, job.id),
				this.serializeJob(job)
			);

			if (job.repeat) {
				// Repeatable job
				this.log("debug", workflowName, job.id, "Repeatable job created.", job);

				await Workflow.rescheduleJob(this, workflowName, job);
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
			await this.subClient.subscribe(this.getKey(workflowName, C.FINISHED));

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
				throw this.broker.errorRegenerator.restore(job2.error, {});
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
	 * @param workflowName
	 * @param jobId
	 * @param promoteAt
	 */
	async sendDelayedJobPromoteAt(
		workflowName: string,
		jobId: string,
		promoteAt: number
	): Promise<void> {
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
	 * Get the next delayed jobs maintenance time.
	 *
	 * @returns
	 */
	async getNextDelayedJobTime(): Promise<number | null> {
		const first = await this.commandClient?.zrange(
			this.getKey(this.wf.name, C.QUEUE_DELAYED),
			0,
			0,
			"WITHSCORES"
		);

		if (first?.length > 0) {
			const promoteAt = parseInt(first[1]);
			if (promoteAt > Date.now()) {
				return promoteAt;
			}
		}

		return null;
	}

	/**
	 * Serialize a job object for storage in Redis.
	 *
	 * @param job - The job object to serialize.
	 * @returns The serialized job object.
	 */
	serializeJob(job: Job): Job {
		const res = { ...job };

		for (const field of C.JOB_FIELDS_JSON) {
			if (job[field] != null) {
				res[field] = this.serializer.serialize(job[field]);
			}
		}

		return res;
	}

	/**
	 * Deserialize a job object retrieved from Redis.
	 *
	 * @param {Job} job - The serialized job object.
	 * @returns {Job} The deserialized job object.
	 */
	deserializeJob(job: Job): Job {
		const res = {};

		for (const [key, value] of Object.entries(job)) {
			if (C.JOB_FIELDS_JSON.includes(key)) {
				if (value != null) {
					// @ts-ignore
					res[key] = value !== "" ? this.serializer.deserialize(value) : null;
				}
			} else if (C.JOB_FIELDS_NUMERIC.includes(key)) {
				if (value != null) {
					res[key] = value !== "" ? Number(value) : null;
				}
			} else if (C.JOB_FIELDS_BOOLEAN.includes(key)) {
				if (value != null) {
					res[key] = String(value) === "true";
				}
			} else {
				if (value != null) {
					res[key] = value !== "" ? String(value) : null;
				}
			}
		}

		return res as Job;
	}

	/**
	 * Finish a parent job.
	 *
	 * @param workflowName
	 * @param jobId
	 */
	async finishParentJob(workflowName: string, jobId: string): Promise<void> {
		await this.commandClient.hmset(this.getKey(workflowName, C.QUEUE_JOB, jobId), {
			finishedAt: Date.now(),
			nodeID: this.broker.nodeID
		});
	}

	/**
	 * Reschedule a repeatable job based on its configuration.
	 *
	 * @param workflowName - The name of workflow.
	 * @param job - The job object or job ID to reschedule.
	 * @returns Resolves when the job is rescheduled.
	 */
	async newRepeatChildJob(workflowName: string, job: Job): Promise<void> {
		job.repeatCounter = await this.commandClient.hincrby(
			this.getKey(workflowName, C.QUEUE_JOB, job.parent),
			"repeatCounter",
			1
		);

		// Save the next scheduled Job to Redis
		await this.commandClient.hmset(
			this.getKey(workflowName, C.QUEUE_JOB, job.id),
			this.serializeJob(job)
		);

		// Push to delayed queue
		await this.commandClient.zadd(
			this.getKey(workflowName, C.QUEUE_DELAYED),
			job.promoteAt,
			job.id
		);

		await this.sendDelayedJobPromoteAt(workflowName, job.id, job.promoteAt);
	}

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 * If both are provided, the adapter should clean up only the job with the given ID.
	 * If only the workflow name is provided, the adapter should clean up all jobs
	 * related to that workflow.
	 * If neither is provided, the adapter should clean up all jobs.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns
	 */
	async cleanUp(workflowName?: string, jobId?: string): Promise<void> {
		if (workflowName && jobId) {
			const pipeline = this.commandClient.pipeline();
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB, jobId));
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB_LOCK, jobId));
			pipeline.del(this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId));
			pipeline.lrem(this.getKey(workflowName, C.QUEUE_WAITING), 1, jobId);
			await pipeline.exec();
			this.log("info", workflowName, jobId, "Cleaned up job store.");
		} else if (workflowName) {
			await this.deleteKeys(this.getKey(workflowName) + ":*");
			this.log("info", workflowName, null, "Cleaned up workflow store.");
		} else {
			await this.deleteKeys(this.prefix + ":*");
			this.logger.info(`Cleaned up entire store.`);
		}
	}

	/**
	 * Delete keys from Redis based on a pattern.
	 *
	 * @param pattern
	 * @returns
	 */
	async deleteKeys(pattern: string): Promise<void> {
		if (this.commandClient instanceof Redis.Cluster) {
			return this.deleteKeysOnCluster(pattern);
		} else {
			return this.deleteKeysOnNode(this.commandClient, pattern);
		}
	}

	/**
	 * Delete keys on a Redis cluster.
	 *
	 * @param pattern
	 * @returns
	 */
	async deleteKeysOnCluster(pattern: string): Promise<void> {
		// get only master nodes to scan for deletion,
		// if we get slave nodes, it would be failed for deletion.
		return Promise.all(
			this.commandClient
				.nodes("master")
				.map(async node => this.deleteKeysOnNode(node, pattern))
		);
	}

	/**
	 * Delete keys on a Redis node.
	 *
	 * @param node
	 * @param pattern
	 * @returns
	 */
	async deleteKeysOnNode(node: Redis, pattern: string): Promise<void> {
		return new Promise((resolve, reject) => {
			const stream = node.scanStream({
				match: pattern,
				count: 100
			});

			stream.on("data", async (keys = []) => {
				if (!keys.length) return;

				stream.pause();
				try {
					await node.del(keys);
					stream.resume();
				} catch (err) {
					err.pattern = pattern;
					return reject(err);
				}
			});

			stream.on("error", err => {
				this.logger.error(
					`Error occured while deleting keys '${pattern}' on Redis node.`,
					err
				);
				reject(err);
			});

			stream.on("end", () => resolve());
		});
	}

	/**
	 * Acquire a maintenance lock for a workflow.
	 *
	 * @param lockTime - The time to hold the lock in milliseconds.
	 * @param lockName - The name of the lock.
	 * @returns Resolves with true if the lock is acquired, false otherwise.
	 */
	async lockMaintenance(
		lockTime: number,
		lockName: string = C.QUEUE_MAINTENANCE_LOCK
	): Promise<boolean> {
		// Set lock
		const lockRes = await this.commandClient?.set(
			this.getKey(this.wf.name, lockName),
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
	 * @returns {Promise<void>} Resolves when delayed jobs are processed.
	 */
	async maintenanceDelayedJobs() {
		this.log("debug", this.wf.name, null, "Maintenance delayed jobs...");
		try {
			const now = Date.now();
			const jobIds = await this.commandClient.zrangebyscore(
				this.getKey(this.wf.name, C.QUEUE_DELAYED),
				0,
				now
			);
			if (jobIds.length > 0) {
				try {
					const pipeline = this.commandClient.pipeline();
					for (const jobId of jobIds) {
						const job = await this.getJob(this.wf.name, jobId, ["id"]);
						if (job) {
							this.log(
								"debug",
								this.wf.name,
								jobId,
								"Moving delayed job to waiting queue."
							);
							pipeline.lpush(this.getKey(this.wf.name, C.QUEUE_WAITING), jobId);
						}
						pipeline.zrem(this.getKey(this.wf.name, C.QUEUE_DELAYED), jobId);
					}
					await pipeline.exec();
				} catch (err) {
					this.log(
						"error",
						this.wf.name,
						null,
						"Error while moving delayed jobs to waiting queue",
						err
					);
				}
			}
		} catch (err) {
			this.log("error", this.wf.name, null, "Error while processing delayed jobs", err);
		}
	}

	/**
	 * Check active jobs and if they timed out, move to failed jobs.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	async maintenanceActiveJobs(): Promise<void> {
		this.log("debug", this.wf.name, null, "Maintenance active jobs...");
		try {
			const now = Date.now();
			const activeJobIds = await this.commandClient.lrange(
				this.getKey(this.wf.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				for (const jobId of activeJobIds) {
					try {
						const job = await this.getJob(this.wf.name, jobId, [
							"startedAt",
							"timeout"
						]);
						if (job && job.startedAt > 0) {
							const timeout = parseDuration(
								job.timeout != null ? job.timeout : this.wf.opts.timeout
							);
							if (timeout > 0) {
								if (now - job.startedAt > timeout) {
									this.log(
										"debug",
										this.wf.name,
										jobId,
										`Job timed out (${humanize(timeout)}). Moving to failed queue.`
									);
									await this.moveToFailed(
										jobId,
										new WorkflowTimeoutError(this.wf.name, jobId, timeout)
									);
									// Unlock manually
									await this.commandClient.del(
										this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId)
									);

									this.wf.removeRunningJob(jobId);
								}
							}
						}
					} catch (err) {
						this.log(
							"error",
							this.wf.name,
							jobId,
							"Error while timeout active job",
							err
						);
					}
				}
			}
		} catch (err) {
			this.log("error", this.wf.name, null, "Error while processing active jobs", err);
		}
	}

	/**
	 * Process stalled jobs for a workflow and move them back to the waiting queue.
	 *
	 * @returns Resolves when stalled jobs are processed.
	 */
	async maintenanceStalledJobs(): Promise<void> {
		this.log("debug", this.wf.name, null, "Maintenance stalled jobs...");

		try {
			const stalledJobIds = await this.commandClient.smembers(
				this.getKey(this.wf.name, C.QUEUE_STALLED)
			);
			if (stalledJobIds.length > 0) {
				for (const jobId of stalledJobIds) {
					try {
						// Check the job lock is exists
						const exists = await this.commandClient.exists(
							this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId)
						);
						if (exists == 0) {
							// Check the job is in the active queue
							const removed = await this.commandClient.lrem(
								this.getKey(this.wf.name, C.QUEUE_ACTIVE),
								1,
								jobId
							);
							if (removed > 0) {
								// Move the job back to the waiting queue
								await this.moveStalledJobsToWaiting(jobId);
							}
						}

						// Remove from stalled queue
						await this.commandClient.srem(
							this.getKey(this.wf.name, C.QUEUE_STALLED),
							jobId
						);
					} catch (err) {
						this.log(
							"error",
							this.wf.name,
							jobId,
							"Error while processing stalled job",
							err
						);
					}
				}
			}

			// Copy active jobIds to stalled queue
			const activeJobIds = await this.commandClient.lrange(
				this.getKey(this.wf.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				await this.commandClient.sadd(
					this.getKey(this.wf.name, C.QUEUE_STALLED),
					activeJobIds
				);
			}
		} catch (err) {
			this.log("error", this.wf.name, null, "Error while processing stalled jobs", err);
		}
	}

	/**
	 * Move stalled job back to the waiting queue.
	 *
	 * @param jobId - The ID of the stalled job.
	 * @returns Resolves when the job is moved to the failed queue.
	 */
	async moveStalledJobsToWaiting(jobId: string): Promise<void> {
		const stalledCounter = await this.commandClient.hincrby(
			this.getKey(this.wf.name, C.QUEUE_JOB, jobId),
			"stalledCounter",
			1
		);

		await this.addJobEvent(this.wf.name, jobId, { type: "stalled" });
		this.sendJobEvent(this.wf.name, jobId, "stalled");

		if (this.wf.opts.maxStalledCount > 0 && stalledCounter > this.wf.opts.maxStalledCount) {
			this.log(
				"debug",
				this.wf.name,
				jobId,
				"Job is reached the maximum stalled count. It's moved to failed."
			);

			await this.moveToFailed(
				jobId,
				new WorkflowMaximumStalled(jobId, this.wf.opts.maxStalledCount)
			);
			return;
		}

		this.log("debug", this.wf.name, jobId, `Job is stalled. Moved back to waiting queue.`);
		await this.commandClient.lpush(this.getKey(this.wf.name, C.QUEUE_WAITING), jobId);
	}

	/**
	 * Remove old jobs from a specified queue based on their age.
	 *
	 * @param queueName - The name of the queue (e.g., completed, failed).
	 * @param retention - The age threshold in milliseconds for removing jobs.
	 * @returns Resolves when old jobs are removed.
	 */
	async maintenanceRemoveOldJobs(queueName: string, retention: number): Promise<void> {
		this.log(
			"debug",
			this.wf.name,
			null,
			`Maintenance remove old ${queueName} jobs... (retention: ${humanize(retention)})`
		);

		try {
			const minDate = Date.now() - retention;
			const jobIds = await this.commandClient.zrangebyscore(
				this.getKey(this.wf.name, queueName),
				0,
				minDate
			);
			if (jobIds.length > 0) {
				const jobKeys = jobIds.map(jobId => this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
				const jobEventsKeys = jobIds.map(jobId =>
					this.getKey(this.wf.name, C.QUEUE_JOB_EVENTS, jobId)
				);
				const pipeline = this.commandClient.pipeline();
				pipeline.del(jobKeys);
				pipeline.del(jobEventsKeys);
				pipeline.zremrangebyscore(this.getKey(this.wf.name, queueName), 0, minDate);
				await pipeline.exec();

				this.log(
					"debug",
					this.wf.name,
					null,
					`Removed ${jobIds.length} ${queueName} jobs:`,
					jobIds
				);
			}
		} catch (err) {
			this.log(
				"error",
				this.wf.name,
				null,
				`Error while removing old ${queueName} jobs`,
				err
			);
		}
	}

	/**
	 * Release the maintenance lock for a workflow.
	 *
	 * @param lockName - The name of the lock to release.
	 * @returns Resolves when the lock is released.
	 */
	async unlockMaintenance(lockName: string = C.QUEUE_MAINTENANCE_LOCK): Promise<void> {
		const unlockRes = await this.commandClient?.del(this.getKey(this.wf.name, lockName));
		this.log("debug", this.wf.name, null, "Unlock maintenance", lockName, unlockRes);
	}

	/**
	 * Get Redis key (for Workflow) for the given name and type.
	 *
	 * @param name - The name of the workflow or entity.
	 * @param type - The type of the key (e.g., job, lock, events).
	 * @param id - Optional ID to append to the key.
	 * @returns The constructed Redis key.
	 */
	getKey(name: string, type?: string, id?: string): string {
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
	 * @param signalName The name of the signal
	 * @param key The key of the signal
	 * @returns The constructed Redis key for the signal.
	 */
	getSignalKey(signalName: string, key?: string): string {
		return `${this.prefix}${C.QUEUE_CATEGORY_SIGNAL}:${signalName}:${key}`;
	}

	/**
	 * Format the result of zrange command to an array of objects.
	 *
	 * @param list
	 * @param timeField
	 * @returns
	 */
	formatZrangeResultToObject(
		list: string[],
		timeField: string = "finishedAt"
	): { id: string; [timeField]: number }[] {
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
	 * @param workflowName
	 * @returns
	 */
	async listCompletedJobs(workflowName: string): Promise<{ id: string; finishedAt: number }[]> {
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
	 * @param workflowName
	 * @returns
	 */
	async listFailedJobs(workflowName: string): Promise<{ id: string; finishedAt: number }[]> {
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
	 * @param workflowName
	 * @returns
	 */
	async listDelayedJobs(workflowName): Promise<{ id: string; promoteAt: number }[]> {
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
	 * @param workflowName
	 * @returns
	 */
	async listActiveJobs(workflowName): Promise<{ id: string }[]> {
		return (
			await this.commandClient.lrange(this.getKey(workflowName, C.QUEUE_ACTIVE), 0, -1)
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * List all waiting job IDs for a workflow.
	 * @param  workflowName
	 * @returns
	 */
	async listWaitingJobs(workflowName): Promise<{ id: string }[]> {
		return (
			await this.commandClient.lrange(this.getKey(workflowName, C.QUEUE_WAITING), 0, -1)
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * Dump all Redis data for all workflows to JSON files.
	 *
	 * @param folder - The folder to save the dump files.
	 * @param wfNames - The names of the workflows to dump.
	 */
	async dumpWorkflows(folder: string, wfNames: string[]): Promise<string[]> {
		makeDirs(folder);
		for (const name of wfNames) {
			await this.dumpWorkflow(name, folder);
		}
	}

	/**
	 * Dump all Redis data for a workflow to a JSON file.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param folder - The folder to save the dump files.
	 * @returns Path to the dump file.
	 */
	async dumpWorkflow(workflowName: string, folder: string = "."): Promise<string> {
		const fs = require("fs").promises;
		const path = require("path");

		const pattern = this.getKey(workflowName) + ":*";
		const client = this.commandClient;
		let cursor = "0";
		const keys = [];
		// Scan all keys matching the workflow pattern
		do {
			const res = await client.scan(cursor, "MATCH", pattern, "COUNT", 100);
			cursor = res[0];
			keys.push(...res[1]);
		} while (cursor !== "0");

		const deserializeData = value => {
			if (_.isString(value) || Buffer.isBuffer(value)) {
				try {
					return this.serializer.deserialize(value);
				} catch (e) {
					// If deserialization fails, return the original value
					return value;
				}
			} else if (_.isObject(value)) {
				try {
					return this.deserializeJob(value);
				} catch (e) {
					// If deserialization fails, return the original value
					return value;
				}
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
					: list.map(item => item.toString());
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
