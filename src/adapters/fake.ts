/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

import _ from "lodash";
import { EventEmitter } from "node:events";
import { Serializers, ServiceBroker, Logger } from "moleculer";
import { Utils } from "moleculer";
import { promises as fs } from "node:fs";
import path from "node:path";

import BaseAdapter, { ListJobResult, ListDelayedJobResult, ListFinishedJobResult } from "./base.ts";
import {
	WorkflowError,
	WorkflowAlreadyLocked,
	WorkflowTimeoutError,
	WorkflowMaximumStalled
} from "../errors.ts";
import * as C from "../constants.ts";
import { parseDuration, humanize } from "../utils.ts";
import Workflow from "../workflow.ts";
import type { BaseDefaultOptions } from "./base.ts";
import {
	CreateJobOptions,
	Job,
	JobEvent,
	WorkflowsMiddlewareOptions,
	SignalWaitOptions
} from "../types.ts";

export interface FakeAdapterOptions extends BaseDefaultOptions {
	prefix?: string;
	serializer?: string;
	drainDelay?: number;
}

export type StoredPromise<T = unknown> = {
	promise: Promise<T>;
	resolve: (v: T) => void;
	reject: (e: Error) => void;
};

type StoredString = { value: unknown; expiresAt: number | null };

/**
 * In-memory storage engine shared between adapter instances with the same prefix.
 * It mimics the Redis data structures (hash, list, set, sorted set, string with
 * expiration) and provides an EventEmitter as the Pub/Sub replacement.
 */
export class FakeStorage {
	public hashes: Map<string, Map<string, unknown>>;
	public lists: Map<string, unknown[]>;
	public sets: Map<string, Set<string>>;
	public zsets: Map<string, Map<string, number>>;
	public strings: Map<string, StoredString>;
	public emitter: EventEmitter;

	constructor() {
		this.hashes = new Map();
		this.lists = new Map();
		this.sets = new Map();
		this.zsets = new Map();
		this.strings = new Map();
		this.emitter = new EventEmitter();
		this.emitter.setMaxListeners(0);
	}

	/**
	 * Normalize a value the same way as Redis stores hash field values:
	 * everything is stored as string, except Buffers (serialized content).
	 */
	private normalizeValue(value: unknown): unknown {
		if (Buffer.isBuffer(value)) return value;
		return String(value);
	}

	// --- HASH commands ---

	hmset(key: string, obj: object): void {
		let hash = this.hashes.get(key);
		if (!hash) {
			hash = new Map();
			this.hashes.set(key, hash);
		}
		for (const [field, value] of Object.entries(obj)) {
			if (value === undefined) continue;
			hash.set(field, this.normalizeValue(value));
		}
	}

	hset(key: string, field: string, value: unknown): void {
		this.hmset(key, { [field]: value });
	}

	hsetnx(key: string, field: string, value: unknown): number {
		const hash = this.hashes.get(key);
		if (hash && hash.has(field)) return 0;
		this.hmset(key, { [field]: value });
		return 1;
	}

	hget(key: string, field: string): unknown {
		const hash = this.hashes.get(key);
		if (!hash) return null;
		return hash.has(field) ? hash.get(field) : null;
	}

	hmget(key: string, fields: string[]): unknown[] {
		return fields.map(field => this.hget(key, field));
	}

	hgetall(key: string): Record<string, unknown> {
		const hash = this.hashes.get(key);
		if (!hash) return {};
		return Object.fromEntries(hash.entries());
	}

	hincrby(key: string, field: string, increment: number): number {
		const current = this.hget(key, field);
		const value = (current != null ? parseInt(String(current)) : 0) + increment;
		this.hset(key, field, value);
		return value;
	}

	hdel(key: string, fields: string[]): void {
		const hash = this.hashes.get(key);
		if (!hash) return;
		for (const field of fields) hash.delete(field);
		if (hash.size === 0) this.hashes.delete(key);
	}

	// --- LIST commands ---

	lpush(key: string, value: unknown): void {
		let list = this.lists.get(key);
		if (!list) {
			list = [];
			this.lists.set(key, list);
		}
		list.unshift(value);
	}

	rpush(key: string, value: unknown): void {
		let list = this.lists.get(key);
		if (!list) {
			list = [];
			this.lists.set(key, list);
		}
		list.push(value);
	}

	rpoplpush(source: string, destination: string): unknown {
		const list = this.lists.get(source);
		if (!list || list.length === 0) return null;
		const value = list.pop();
		if (list.length === 0) this.lists.delete(source);
		this.lpush(destination, value);
		return value;
	}

	lrange(key: string, start: number, stop: number): unknown[] {
		const list = this.lists.get(key);
		if (!list) return [];
		return list.slice(start, stop === -1 ? undefined : stop + 1);
	}

	lrem(key: string, count: number, value: unknown): number {
		const list = this.lists.get(key);
		if (!list) return 0;
		let removed = 0;
		const max = count === 0 ? Number.POSITIVE_INFINITY : Math.abs(count);
		// count >= 0: head to tail (count < 0 is not used by the adapter)
		for (let i = 0; i < list.length && removed < max; i++) {
			if (String(list[i]) === String(value)) {
				list.splice(i, 1);
				removed++;
				i--;
			}
		}
		if (list.length === 0) this.lists.delete(key);
		return removed;
	}

	// --- SET commands ---

	sadd(key: string, members: string[]): void {
		let set = this.sets.get(key);
		if (!set) {
			set = new Set();
			this.sets.set(key, set);
		}
		for (const member of members) set.add(member);
	}

	srem(key: string, member: string): void {
		const set = this.sets.get(key);
		if (!set) return;
		set.delete(member);
		if (set.size === 0) this.sets.delete(key);
	}

	smembers(key: string): string[] {
		const set = this.sets.get(key);
		return set ? Array.from(set) : [];
	}

	// --- SORTED SET commands ---

	zadd(key: string, score: number, member: string): void {
		let zset = this.zsets.get(key);
		if (!zset) {
			zset = new Map();
			this.zsets.set(key, zset);
		}
		zset.set(member, score);
	}

	zrem(key: string, member: string): void {
		const zset = this.zsets.get(key);
		if (!zset) return;
		zset.delete(member);
		if (zset.size === 0) this.zsets.delete(key);
	}

	/**
	 * Get all entries of a sorted set ordered by score (ties by member), like Redis.
	 */
	zentries(key: string): Array<[string, number]> {
		const zset = this.zsets.get(key);
		if (!zset) return [];
		return Array.from(zset.entries()).sort(
			(a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)
		);
	}

	zrange(key: string, start: number, stop: number): string[] {
		const entries = this.zentries(key).slice(start, stop === -1 ? undefined : stop + 1);
		return entries.map(([member]) => member);
	}

	zrangeWithScores(key: string, start: number, stop: number): string[] {
		const entries = this.zentries(key).slice(start, stop === -1 ? undefined : stop + 1);
		const res = [];
		for (const [member, score] of entries) {
			res.push(member, String(score));
		}
		return res;
	}

	zrangebyscore(key: string, min: number, max: number): string[] {
		return this.zentries(key)
			.filter(([, score]) => score >= min && score <= max)
			.map(([member]) => member);
	}

	zremrangebyscore(key: string, min: number, max: number): void {
		const zset = this.zsets.get(key);
		if (!zset) return;
		for (const [member, score] of Array.from(zset.entries())) {
			if (score >= min && score <= max) zset.delete(member);
		}
		if (zset.size === 0) this.zsets.delete(key);
	}

	// --- STRING commands (with lazy expiration) ---

	private isExpired(item: StoredString): boolean {
		return item.expiresAt != null && item.expiresAt <= Date.now();
	}

	/**
	 * Set a string value with optional expiration and NX/XX conditions,
	 * like the Redis SET command. Returns "OK" or null.
	 */
	setString(
		key: string,
		value: unknown,
		opts?: { px?: number; nx?: boolean; xx?: boolean }
	): string | null {
		const current = this.strings.get(key);
		const exists = current != null && !this.isExpired(current);

		if (opts?.nx && exists) return null;
		if (opts?.xx && !exists) return null;

		this.strings.set(key, {
			value,
			expiresAt: opts?.px != null ? Date.now() + opts.px : null
		});
		return "OK";
	}

	getString(key: string): unknown {
		const item = this.strings.get(key);
		if (!item) return null;
		if (this.isExpired(item)) {
			this.strings.delete(key);
			return null;
		}
		return item.value;
	}

	// --- Generic commands ---

	exists(key: string): boolean {
		if (this.hashes.has(key)) return true;
		if (this.lists.has(key)) return true;
		if (this.sets.has(key)) return true;
		if (this.zsets.has(key)) return true;
		if (this.strings.has(key)) return this.getString(key) != null;
		return false;
	}

	del(key: string): void {
		this.hashes.delete(key);
		this.lists.delete(key);
		this.sets.delete(key);
		this.zsets.delete(key);
		this.strings.delete(key);
	}

	keys(): string[] {
		return [
			...this.hashes.keys(),
			...this.lists.keys(),
			...this.sets.keys(),
			...this.zsets.keys(),
			...this.strings.keys()
		];
	}
}

/**
 * Fake (in-memory) Adapter for Workflows.
 *
 * It covers the same functionality as the Redis adapter but stores everything
 * in the process memory. Intended for testing & development. The storage is
 * shared between adapter instances with the same prefix, so multiple brokers
 * within the same process can communicate with each other.
 */
export default class FakeAdapter extends BaseAdapter {
	declare opts: FakeAdapterOptions;
	public isWorker: boolean;
	public storage: FakeStorage;
	public signalPromises: Map<string, StoredPromise<unknown>>;
	public jobResultPromises: Map<string, StoredPromise<unknown>>;
	public subscriptions: Map<string, (message: unknown) => void>;

	public running: boolean;
	public popping: boolean;
	public disconnecting: boolean;
	public prefix!: string;
	declare serializer: Serializers.Base;
	declare wf: Workflow;
	declare broker: ServiceBroker;
	declare logger: Logger;
	declare mwOpts: WorkflowsMiddlewareOptions;

	/**
	 * Shared storages by prefix. All adapter instances (worker & client) with the
	 * same prefix use the same storage, similar to a common Redis server.
	 */
	static storages: Map<string, FakeStorage> = new Map();

	/**
	 * Get or create the shared storage for the given prefix.
	 *
	 * @param prefix
	 */
	static getStorage(prefix: string): FakeStorage {
		let storage = FakeAdapter.storages.get(prefix);
		if (!storage) {
			storage = new FakeStorage();
			FakeAdapter.storages.set(prefix, storage);
		}
		return storage;
	}

	/**
	 * Remove all shared storages. Useful in tests.
	 */
	static clearAll(): void {
		FakeAdapter.storages.clear();
	}

	/**
	 * Constructor of adapter.
	 */
	constructor(opts?: FakeAdapterOptions) {
		super(opts);

		this.opts = _.defaultsDeep(this.opts, {
			serializer: "JSON",
			drainDelay: 5
		});

		this.isWorker = false;
		this.storage = null;

		this.signalPromises = new Map();
		this.jobResultPromises = new Map();
		this.subscriptions = new Map();

		this.running = false;
		this.popping = false;
		this.disconnecting = false;
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
		logger: Logger,
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

		this.logger.debug("Workflows Fake adapter prefix:", this.prefix);

		// create an instance of serializer (default to JSON)
		this.serializer = Serializers.resolve(this.opts.serializer);
		this.serializer.init(this.broker);
		this.logger.info("Workflows serializer:", this.broker.getConstructorName(this.serializer));
	}

	/**
	 * Connect to the shared in-memory storage.
	 *
	 * @returns Resolves when the connection is established.
	 */
	async connect(): Promise<void> {
		if (this.connected) return;

		this.storage = FakeAdapter.getStorage(this.prefix);

		if (this.isWorker) {
			this.subscribe(this.getKey(this.wf.name, C.QUEUE_DELAYED));
		}

		this.connected = true;
		this.log("info", this.wf?.name ?? "", null, "Fake adapter connected.");
	}

	/**
	 * Close the adapter connection.
	 *
	 * @returns Resolves when the disconnection is complete.
	 */
	async disconnect(): Promise<void> {
		if (this.disconnecting) return;

		this.disconnecting = true;
		this.connected = false;

		if (this.storage) {
			for (const [channel, listener] of this.subscriptions.entries()) {
				this.storage.emitter.off(channel, listener);
			}
			this.subscriptions.clear();

			// Release a possibly blocked job processor
			if (this.isWorker && this.wf) {
				this.storage.emitter.emit("wake:" + this.getKey(this.wf.name, C.QUEUE_WAITING));
			}
		}

		this.disconnecting = false;
		this.log("info", this.wf?.name ?? "", null, "Fake adapter disconnected.");
	}

	/**
	 * Subscribe this instance to a Pub/Sub channel on the shared emitter.
	 *
	 * @param channel
	 */
	subscribe(channel: string): void {
		if (this.subscriptions.has(channel)) return;

		const listener = (message: unknown) =>
			this.onChannelMessage(channel, message as Buffer | string);
		this.subscriptions.set(channel, listener);
		this.storage.emitter.on(channel, listener);
	}

	/**
	 * Publish a serialized message to a Pub/Sub channel. The delivery is
	 * asynchronous, similar to Redis Pub/Sub.
	 *
	 * @param channel
	 * @param content - Serialized message content.
	 */
	publish(channel: string, content: Buffer | string): void {
		const storage = this.storage;
		if (!storage) return;
		setImmediate(() => storage.emitter.emit(channel, content));
	}

	/**
	 * Handle an incoming Pub/Sub message on a subscribed channel.
	 *
	 * @param channel
	 * @param message
	 */
	onChannelMessage(channel: string, message: Buffer | string): void {
		if (channel.startsWith(this.getKey(C.QUEUE_CATEGORY_SIGNAL + ":"))) {
			const json = this.serializer.deserialize(message as Buffer);
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
			const json = this.serializer.deserialize(message as Buffer);
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
				const json = this.serializer.deserialize(message as Buffer);
				this.wf.setNextDelayedMaintenance(json.promoteAt);
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

		// Release a possibly blocked job processor
		if (this.storage && this.wf) {
			this.storage.emitter.emit("wake:" + this.getKey(this.wf.name, C.QUEUE_WAITING));
		}
	}

	/**
	 * Pop a job from the waiting queue and move it to the active queue.
	 * If the waiting queue is empty, it waits for a new job or until the
	 * timeout elapses (like Redis BRPOPLPUSH).
	 *
	 * @param timeout - Maximum wait time in seconds.
	 * @returns Resolves with the job ID or null on timeout.
	 */
	async popWaitingJob(timeout: number): Promise<string | null> {
		const waitingKey = this.getKey(this.wf.name, C.QUEUE_WAITING);
		const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);

		let jobId = this.storage.rpoplpush(waitingKey, activeKey);
		if (jobId != null) return jobId as string;

		// Wait for a new waiting job or the drain delay
		await new Promise<void>(resolve => {
			const wakeEvent = "wake:" + waitingKey;
			const onWake = () => {
				clearTimeout(timer);
				resolve();
			};
			const timer = setTimeout(() => {
				this.storage.emitter.off(wakeEvent, onWake);
				resolve();
			}, timeout * 1000);
			this.storage.emitter.once(wakeEvent, onWake);
		});

		if (!this.running || !this.connected) return null;

		jobId = this.storage.rpoplpush(waitingKey, activeKey);
		return jobId != null ? (jobId as string) : null;
	}

	/**
	 * Job processor for the given workflow. Waits for a job in the waiting queue,
	 * moves it to the active queue, and processes it.
	 *
	 * @returns Resolves when the job is processed.
	 */
	async runJobProcessor(): Promise<void> {
		if (!this.running || this.popping) return;

		const concurrency = this.wf.opts.concurrency;
		const activeJobCount = this.wf.getNumberOfActiveJobs();

		if (activeJobCount >= concurrency) {
			return;
		}

		try {
			this.popping = true;
			const jobId = await this.popWaitingJob(this.opts.drainDelay);
			this.popping = false;

			if (jobId) {
				this.wf.addRunningJob(jobId);
				// Process the job, without awaiting for it
				this.processJob(jobId);
			}
		} catch (err) {
			// Swallow error if disconnecting
			if (!this.disconnecting) {
				this.log("error", this.wf.name, null, "Unable to watch job", err);
			}
		} finally {
			this.popping = false;
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
		const job = await this.getJob(this.wf.name, jobId);
		if (!job) {
			this.log("warn", this.wf.name, jobId, "Job not found");
			// Remove from active queue
			this.storage.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, jobId);

			this.wf.removeRunningJob(jobId);

			return;
		}

		const jobEvents = await this.getJobEvents(this.wf.name, jobId);

		if (!job.startedAt) {
			job.startedAt = Date.now();
		}

		let unlock;
		try {
			unlock = await this.lock(jobId);

			this.storage.hsetnx(
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
	 * @param jobId - The ID of the job to lock.
	 * @returns Resolves with a function to unlock the job.
	 */
	async lock(jobId: string): Promise<() => Promise<void>> {
		// Set lock
		const lockRes = this.storage.setString(
			this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId),
			this.broker.instanceID,
			{ px: this.mwOpts.lockExpiration * 1000, nx: true }
		);
		this.log("debug", this.wf.name, jobId, "Lock result", lockRes);

		if (!lockRes) throw new WorkflowError(`Job ${jobId} is already locked.`);

		const lockExtender = async () => {
			if (this.disconnecting || !this.connected) {
				clearInterval(timer);
				return;
			}

			// Check if the job is not finished (e.g timeout maintainer process is closed)
			const finishedAt = this.storage.hget(
				this.getKey(this.wf.name, C.QUEUE_JOB, jobId),
				"finishedAt"
			);

			if (Number(finishedAt) > 0) {
				this.log("debug", this.wf.name, jobId, "Job is finished. Unlocking...");
				clearInterval(timer);
				this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId));
				return;
			}

			// Extend the lock
			this.log("debug", this.wf.name, jobId, "Extending lock");
			const lockRes = this.storage.setString(
				this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId),
				this.broker.instanceID,
				{ px: this.mwOpts.lockExpiration * 1000, xx: true }
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

			this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId));
			this.log("debug", this.wf.name, jobId, "Unlocked");
		};
	}

	/**
	 * Get a job from the storage.
	 *
	 * @param workflowName - Workflow name
	 * @param jobId - The ID of the job.
	 * @param fields - The fields to retrieve or true to retrieve all fields.
	 * @returns Resolves with the job object or null if not found.
	 */
	async getJob(
		workflowName: string,
		jobId: string,
		fields?: string[] | true
	): Promise<Job | null> {
		if (fields == null) {
			fields = ["payload", "parent", "startedAt", "retries", "retryAttempts", "timeout"];
		}

		const exists = this.storage.exists(this.getKey(workflowName, C.QUEUE_JOB, jobId));
		if (!exists) return null;

		let job;

		if (fields === true) {
			job = this.storage.hgetall(this.getKey(workflowName, C.QUEUE_JOB, jobId));
		} else {
			const values = this.storage.hmget(
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
	 * Get job events from the storage.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @returns Resolves with an array of job events.
	 */
	async getJobEvents(workflowName: string, jobId: string): Promise<JobEvent[]> {
		const jobEvents = this.storage.lrange(
			this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId),
			0,
			-1
		);

		return jobEvents.map(e => this.serializer.deserialize(e as Buffer) as JobEvent);
	}

	/**
	 * Add a job event to the storage.
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

		this.storage.rpush(
			this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId),
			this.serializer.serialize({
				...event,
				ts: Date.now(),
				nodeID: this.broker.nodeID
			})
		);
	}

	/**
	 * Notify the workers that a new job is available in the waiting queue.
	 *
	 * @param workflowName
	 */
	wakeUpWorkers(workflowName: string): void {
		this.storage.emitter.emit("wake:" + this.getKey(workflowName, C.QUEUE_WAITING));
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

		this.storage.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, job.id);
		this.storage.srem(this.getKey(this.wf.name, C.QUEUE_STALLED), job.id);

		if (this.wf.opts.removeOnCompleted) {
			this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB, job.id));
		} else {
			const fields: Partial<Job> = {
				success: true,
				finishedAt: Date.now(),
				nodeID: this.broker.nodeID,
				duration: Date.now() - job.startedAt
			};
			if (result != null) {
				fields.result = this.serializer.serialize(result);
			}
			this.storage.hmset(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), fields);
			this.storage.zadd(
				this.getKey(this.wf.name, C.QUEUE_COMPLETED),
				fields.finishedAt,
				job.id
			);
		}

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.resolve(result);
		} else {
			this.publish(
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
			error: this.broker.errorRegenerator.extractPlainError(err)
		});

		this.sendJobEvent(this.wf.name, job.id, "finished");
		this.sendJobEvent(this.wf.name, job.id, "failed");

		this.storage.lrem(this.getKey(this.wf.name, C.QUEUE_ACTIVE), 1, job.id);
		this.storage.srem(this.getKey(this.wf.name, C.QUEUE_STALLED), job.id);

		// @ts-expect-error retryable property is not exist on Error but on MoleculerError
		if (err?.retryable) {
			const retryFields = this.storage.hmget(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), [
				"retries",
				"retryAttempts"
			]);
			const retries =
				(retryFields[0] != null
					? parseInt(String(retryFields[0]))
					: this.wf.opts.retryPolicy?.retries) ?? 0;
			const retryAttempts = parseInt(String(retryFields[1] ?? "0"));

			if (retries > 0 && retryAttempts < retries) {
				this.storage.hincrby(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), "retryAttempts", 1);
				const backoffTime = this.getBackoffTime(retryAttempts);
				this.log(
					"debug",
					this.wf.name,
					job.id,
					`Retrying job (${retryAttempts} attempts of ${retries}) after ${backoffTime} ms...`
				);

				const promoteAt = Date.now() + backoffTime;
				this.storage.hset(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), "promoteAt", promoteAt);
				this.storage.zadd(this.getKey(this.wf.name, C.QUEUE_DELAYED), promoteAt, job.id);

				// Publish promoteAt time to all workers
				await this.sendDelayedJobPromoteAt(this.wf.name, job.id, promoteAt);

				return;
			}
		}

		if (this.wf.opts.removeOnFailed) {
			this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB, job.id));
		} else {
			const fields: Partial<Job> = {
				success: false,
				finishedAt: Date.now(),
				nodeID: this.broker.nodeID,
				duration: Date.now() - job.startedAt
			};
			if (err != null) {
				// @ts-expect-error it's not a real job object, just to store properties
				fields.error = this.serializer.serialize(
					this.broker.errorRegenerator.extractPlainError(err)
				);
			}

			this.log("debug", this.wf.name, job.id, `Job move to failed queue.`);

			this.storage.hmset(this.getKey(this.wf.name, C.QUEUE_JOB, job.id), fields);
			this.storage.zadd(this.getKey(this.wf.name, C.QUEUE_FAILED), fields.finishedAt, job.id);
		}

		if (this.jobResultPromises.has(job.id)) {
			const storePromise = this.jobResultPromises.get(job.id);
			this.jobResultPromises.delete(job.id);
			storePromise.reject(err);
		} else {
			this.publish(
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
		this.storage.hset(
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
		const state = this.storage.hget(this.getKey(workflowName, C.QUEUE_JOB, jobId), "state");

		return state != null ? this.serializer.deserialize(state as Buffer) : null;
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
			this.storage.setString(this.getSignalKey(signalName, key), content, { px: exp });
		} else {
			this.storage.setString(this.getSignalKey(signalName, key), content);
		}

		this.publish(this.getKey(C.QUEUE_CATEGORY_SIGNAL, signalName), content);
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

		this.storage.del(this.getSignalKey(signalName, key));
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param signalName - The name of the signal.
	 * @param key - The key associated with the signal.
	 * @param opts Options for waiting for the signal.
	 * @returns Resolves with the signal payload.
	 */
	async waitForSignal<TSignalResult = unknown>(
		signalName: string,
		key?: string,
		// eslint-disable-next-line @typescript-eslint/no-unused-vars
		opts?: SignalWaitOptions
	): Promise<TSignalResult> {
		if (key == null) key = C.SIGNAL_EMPTY_KEY;
		const content = this.storage.getString(this.getSignalKey(signalName, key));
		if (content) {
			const json = this.serializer.deserialize(content as Buffer);
			if (key === json.key) {
				this.logger.debug("Signal received", signalName, key, json);
				return json.payload;
			}
		}

		const pKey = signalName + ":" + key;
		const found = this.signalPromises.get(pKey);
		if (found) {
			return found.promise as Promise<TSignalResult>;
		}

		this.subscribe(this.getKey(C.QUEUE_CATEGORY_SIGNAL, signalName));

		const item = { promise: null, resolve: null } as StoredPromise<TSignalResult>;
		item.promise = new Promise<TSignalResult>(resolve => (item.resolve = resolve));
		this.signalPromises.set(pKey, item);
		this.logger.debug("Waiting for signal", signalName, key);

		return item.promise;
	}

	/**
	 * Create a new job and push it to the waiting or delayed queue.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param job - The job.
	 * @param opts - Additional options for the job.
	 * @returns Resolves with the created job object.
	 */
	async newJob(workflowName: string, job: Job, opts: Partial<CreateJobOptions>) {
		opts = opts || {};

		let isLoadedJob = false;

		if (opts.isCustomJobId) {
			// Job ID collision check
			const exists = this.storage.exists(this.getKey(workflowName, C.QUEUE_JOB, job.id));
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
						this.storage.hdel(
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
			// Save the Job to the storage
			this.storage.hmset(
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
				this.storage.zadd(
					this.getKey(workflowName, C.QUEUE_DELAYED),
					job.promoteAt,
					job.id
				);

				// Publish promoteAt time to all workers
				await this.sendDelayedJobPromoteAt(workflowName, job.id, job.promoteAt);
			} else {
				// Normal job
				this.log("debug", workflowName, job.id, "Job created.", job);
				this.storage.lpush(this.getKey(workflowName, C.QUEUE_WAITING), job.id);
				this.wakeUpWorkers(workflowName);
			}

			this.sendJobEvent(workflowName, job.id, "created");
		}

		job.promise = async () => {
			// Subscribe to the job finished event
			this.subscribe(this.getKey(workflowName, C.FINISHED));

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
			const storePromise = {} as StoredPromise;
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
		const head = this.storage.zrange(this.getKey(workflowName, C.QUEUE_DELAYED), 0, 0);
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
			this.publish(this.getKey(workflowName, C.QUEUE_DELAYED), msg);
		}
	}

	/**
	 * Get the next delayed jobs maintenance time.
	 *
	 * @returns
	 */
	async getNextDelayedJobTime(): Promise<number | null> {
		const first = this.storage?.zentries(this.getKey(this.wf.name, C.QUEUE_DELAYED));

		if (first?.length > 0) {
			const promoteAt = first[0][1];
			if (promoteAt > Date.now()) {
				return promoteAt;
			}
		}

		return null;
	}

	/**
	 * Finish a parent job.
	 *
	 * @param workflowName
	 * @param jobId
	 */
	async finishParentJob(workflowName: string, jobId: string): Promise<void> {
		this.storage.hmset(this.getKey(workflowName, C.QUEUE_JOB, jobId), {
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
		job.repeatCounter = this.storage.hincrby(
			this.getKey(workflowName, C.QUEUE_JOB, job.parent),
			"repeatCounter",
			1
		);

		// Save the next scheduled Job to the storage
		this.storage.hmset(this.getKey(workflowName, C.QUEUE_JOB, job.id), this.serializeJob(job));

		// Push to delayed queue
		this.storage.zadd(this.getKey(workflowName, C.QUEUE_DELAYED), job.promoteAt, job.id);

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
			this.storage.del(this.getKey(workflowName, C.QUEUE_JOB, jobId));
			this.storage.del(this.getKey(workflowName, C.QUEUE_JOB_LOCK, jobId));
			this.storage.del(this.getKey(workflowName, C.QUEUE_JOB_EVENTS, jobId));
			this.storage.lrem(this.getKey(workflowName, C.QUEUE_WAITING), 1, jobId);
			this.log("info", workflowName, jobId, "Cleaned up job store.");
		} else if (workflowName) {
			this.deleteKeys(this.getKey(workflowName) + ":*");
			this.log("info", workflowName, null, "Cleaned up workflow store.");
		} else {
			this.deleteKeys(this.prefix + "*");
			this.logger.info(`Cleaned up entire store.`);
		}
	}

	/**
	 * Delete keys from the storage based on a pattern (prefix match).
	 *
	 * @param pattern
	 * @returns
	 */
	deleteKeys(pattern: string): void {
		const prefix = pattern.endsWith("*") ? pattern.slice(0, -1) : pattern;
		for (const key of this.storage.keys()) {
			if (key.startsWith(prefix)) {
				this.storage.del(key);
			}
		}
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
		const lockRes = this.storage?.setString(
			this.getKey(this.wf.name, lockName),
			this.broker.instanceID,
			{ px: lockTime / 2, nx: true }
		);

		return lockRes != null;
	}

	/**
	 * Release the maintenance lock for a workflow.
	 *
	 * @param lockName - The name of the lock to release.
	 * @returns Resolves when the lock is released.
	 */
	async unlockMaintenance(lockName: string = C.QUEUE_MAINTENANCE_LOCK): Promise<void> {
		this.storage?.del(this.getKey(this.wf.name, lockName));
		this.log("debug", this.wf.name, null, "Unlock maintenance", lockName);
	}

	/**
	 * Process delayed jobs for a workflow and move them to the waiting queue if ready.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	async maintenanceDelayedJobs() {
		this.log("debug", this.wf.name, null, "Maintenance delayed jobs...");
		try {
			const now = Date.now();
			const jobIds = this.storage.zrangebyscore(
				this.getKey(this.wf.name, C.QUEUE_DELAYED),
				0,
				now
			);
			if (jobIds.length > 0) {
				try {
					let pushed = false;
					for (const jobId of jobIds) {
						const job = await this.getJob(this.wf.name, jobId, ["id"]);
						if (job) {
							this.log(
								"debug",
								this.wf.name,
								jobId,
								"Moving delayed job to waiting queue."
							);
							this.storage.lpush(this.getKey(this.wf.name, C.QUEUE_WAITING), jobId);
							pushed = true;
						}
						this.storage.zrem(this.getKey(this.wf.name, C.QUEUE_DELAYED), jobId);
					}
					if (pushed) {
						this.wakeUpWorkers(this.wf.name);
					}
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
			const activeJobIds = this.storage.lrange(
				this.getKey(this.wf.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				for (const jobId of activeJobIds as string[]) {
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
									this.storage.del(
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
			const stalledJobIds = this.storage.smembers(
				this.getKey(this.wf.name, C.QUEUE_STALLED)
			);
			if (stalledJobIds.length > 0) {
				for (const jobId of stalledJobIds) {
					try {
						// Check the job lock is exists
						const exists = this.storage.exists(
							this.getKey(this.wf.name, C.QUEUE_JOB_LOCK, jobId)
						);
						if (!exists) {
							// Check the job is in the active queue
							const removed = this.storage.lrem(
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
						this.storage.srem(this.getKey(this.wf.name, C.QUEUE_STALLED), jobId);
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
			const activeJobIds = this.storage.lrange(
				this.getKey(this.wf.name, C.QUEUE_ACTIVE),
				0,
				-1
			);
			if (activeJobIds.length > 0) {
				this.storage.sadd(
					this.getKey(this.wf.name, C.QUEUE_STALLED),
					activeJobIds as string[]
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
		const stalledCounter = this.storage.hincrby(
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
		this.storage.lpush(this.getKey(this.wf.name, C.QUEUE_WAITING), jobId);
		this.wakeUpWorkers(this.wf.name);
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
			const jobIds = this.storage.zrangebyscore(
				this.getKey(this.wf.name, queueName),
				0,
				minDate
			);
			if (jobIds.length > 0) {
				for (const jobId of jobIds) {
					this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
					this.storage.del(this.getKey(this.wf.name, C.QUEUE_JOB_EVENTS, jobId));
				}
				this.storage.zremrangebyscore(this.getKey(this.wf.name, queueName), 0, minDate);

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
	 * Get storage key (for Workflow) for the given name and type.
	 *
	 * @param name - The name of the workflow or entity.
	 * @param type - The type of the key (e.g., job, lock, events).
	 * @param id - Optional ID to append to the key.
	 * @returns The constructed key.
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
	 * Get storage key for a signal.
	 *
	 * @param signalName The name of the signal
	 * @param key The key of the signal
	 * @returns The constructed key for the signal.
	 */
	getSignalKey(signalName: string, key?: string): string {
		return `${this.prefix}${C.QUEUE_CATEGORY_SIGNAL}:${signalName}:${key}`;
	}

	/**
	 * List all completed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listCompletedJobs(workflowName: string): Promise<ListFinishedJobResult[]> {
		return this.formatZrangeResultToObject<ListFinishedJobResult>(
			this.storage.zrangeWithScores(this.getKey(workflowName, C.QUEUE_COMPLETED), 0, -1)
		);
	}

	/**
	 * List all failed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listFailedJobs(workflowName: string): Promise<ListFinishedJobResult[]> {
		return this.formatZrangeResultToObject<ListFinishedJobResult>(
			this.storage.zrangeWithScores(this.getKey(workflowName, C.QUEUE_FAILED), 0, -1)
		);
	}

	/**
	 * List all delayed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listDelayedJobs(workflowName): Promise<ListDelayedJobResult[]> {
		return this.formatZrangeResultToObject<ListDelayedJobResult>(
			this.storage.zrangeWithScores(this.getKey(workflowName, C.QUEUE_DELAYED), 0, -1),
			"promoteAt"
		);
	}

	/**
	 * List all active job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listActiveJobs(workflowName): Promise<ListJobResult[]> {
		return (
			this.storage.lrange(this.getKey(workflowName, C.QUEUE_ACTIVE), 0, -1) as string[]
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * List all waiting job IDs for a workflow.
	 * @param  workflowName
	 * @returns
	 */
	async listWaitingJobs(workflowName): Promise<ListJobResult[]> {
		return (
			this.storage.lrange(this.getKey(workflowName, C.QUEUE_WAITING), 0, -1) as string[]
		).map(jobId => {
			return { id: jobId };
		});
	}

	/**
	 * Dump all data for all workflows to JSON files.
	 *
	 * @param folder - The folder to save the dump files.
	 * @param wfNames - The names of the workflows to dump.
	 */
	async dumpWorkflows(folder: string, wfNames: string[]) {
		Utils.makeDirs(folder);
		for (const name of wfNames) {
			await this.dumpWorkflow(name, folder);
		}
	}

	/**
	 * Dump all data for a workflow to a JSON file.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param folder - The folder to save the dump files.
	 * @returns Path to the dump file.
	 */
	async dumpWorkflow(workflowName: string, folder: string = ".") {
		const keyPrefix = this.getKey(workflowName) + ":";

		const deserializeData = value => {
			if (_.isString(value) || Buffer.isBuffer(value)) {
				try {
					return this.serializer.deserialize(value as Buffer);
				} catch {
					// If deserialization fails, return the original value
					return value;
				}
			} else if (_.isObject(value)) {
				try {
					return this.deserializeJob(value as Job);
				} catch {
					// If deserialization fails, return the original value
					return value;
				}
			}

			return value;
		};

		const dump = {};

		for (const [key, list] of this.storage.lists.entries()) {
			if (!key.startsWith(keyPrefix)) continue;
			dump[key] = key.includes(":" + C.QUEUE_JOB_EVENTS + ":")
				? list.map(item => deserializeData(item))
				: list.map(item => String(item));
		}

		for (const [key, set] of this.storage.sets.entries()) {
			if (!key.startsWith(keyPrefix)) continue;
			dump[key] = Array.from(set);
		}

		for (const key of this.storage.hashes.keys()) {
			if (!key.startsWith(keyPrefix)) continue;
			dump[key] = deserializeData(this.storage.hgetall(key));
		}

		for (const key of this.storage.zsets.keys()) {
			if (!key.startsWith(keyPrefix)) continue;
			dump[key] = this.storage.zentries(key).map(([member, score]) => ({ member, score }));
		}

		for (const key of this.storage.strings.keys()) {
			if (!key.startsWith(keyPrefix)) continue;
			dump[key] = deserializeData(this.storage.getString(key));
		}

		const fileName = path.join(folder, `dump-${workflowName}.json`);
		await fs.writeFile(fileName, JSON.stringify(dump, null, 2), "utf8");
		this.logger.info(`Workflow '${workflowName}' dumped to ${fileName}`);
		return fileName;
	}
}
