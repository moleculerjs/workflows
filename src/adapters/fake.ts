/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

import _ from "lodash";
import { ServiceBroker, Logger, Utils } from "moleculer";
import BaseAdapter, { ListJobResult, ListDelayedJobResult, ListFinishedJobResult } from "./base.ts";
import { WorkflowTimeoutError } from "../errors.ts";
import * as C from "../constants.ts";
import { parseDuration } from "../utils.ts";
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
}

export type StoredPromise<T = unknown> = {
	promise: Promise<T>;
	resolve: (v: T) => void;
	reject: (e: Error) => void;
};

/**
 * Fake Adapter for Workflows - stores all data in memory
 */
export default class FakeAdapter extends BaseAdapter {
	declare opts: FakeAdapterOptions;
	public isWorker: boolean;
	public running: boolean;
	public disconnecting: boolean;
	public prefix!: string;
	declare wf: Workflow;
	declare broker: ServiceBroker;
	declare logger: Logger;
	declare mwOpts: WorkflowsMiddlewareOptions;

	// Shared promise storage across all adapter instances
	private static sharedSignalPromises: Map<string, StoredPromise<unknown>> = new Map();
	private static sharedJobResultPromises: Map<string, StoredPromise<unknown>> = new Map();

	// Instance accessors for shared promise storage
	public get signalPromises() {
		return FakeAdapter.sharedSignalPromises;
	}
	public get jobResultPromises() {
		return FakeAdapter.sharedJobResultPromises;
	}

	// Shared in-memory storage across all adapter instances
	private static sharedJobs: Map<string, Job> = new Map(); // key: workflowName:jobId
	private static sharedJobEvents: Map<string, JobEvent[]> = new Map(); // key: workflowName:jobId
	private static sharedJobStates: Map<string, unknown> = new Map(); // key: workflowName:jobId
	private static sharedSignals: Map<string, unknown> = new Map(); // key: signalName:key
	private static sharedQueues: Map<string, Set<string>> = new Map(); // key: workflowName:queueType, value: Set of jobIds
	private static sharedDelayedJobs: Map<string, { jobId: string; promoteAt: number }[]> =
		new Map(); // key: workflowName
	private static sharedLocks: Map<string, { lockedAt: number; lockTime: number }> = new Map(); // key: lockName

	// Instance accessors for shared storage
	private get jobs() {
		return FakeAdapter.sharedJobs;
	}
	private get jobEvents() {
		return FakeAdapter.sharedJobEvents;
	}
	private get jobStates() {
		return FakeAdapter.sharedJobStates;
	}
	private get signals() {
		return FakeAdapter.sharedSignals;
	}
	private get queues() {
		return FakeAdapter.sharedQueues;
	}
	private get delayedJobs() {
		return FakeAdapter.sharedDelayedJobs;
	}
	private get locks() {
		return FakeAdapter.sharedLocks;
	}

	/**
	 * Constructor of adapter.
	 */
	constructor(opts?: FakeAdapterOptions) {
		super(opts);

		this.opts = _.defaultsDeep(this.opts, {});

		this.isWorker = false;
		this.running = false;
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
	}

	/**
	 * Connect to the adapter.
	 *
	 * @returns Resolves when the connection is established.
	 */
	async connect(): Promise<void> {
		if (this.connected) return;

		this.connected = true;
		this.log("info", this.wf?.name ?? "", null, "Fake adapter connected.");
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

		// Clear shared storage only if this is the last instance
		// For simplicity, we'll just clear it every time for now
		FakeAdapter.sharedJobs.clear();
		FakeAdapter.sharedJobEvents.clear();
		FakeAdapter.sharedJobStates.clear();
		FakeAdapter.sharedSignals.clear();
		FakeAdapter.sharedQueues.clear();
		FakeAdapter.sharedDelayedJobs.clear();
		FakeAdapter.sharedLocks.clear();

		this.disconnecting = false;
		this.log("info", this.wf?.name ?? "", null, "Fake adapter disconnected.");
	}

	/**
	 * Generate a key for storage.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param type - The type of the key (optional).
	 * @param id - The ID (optional).
	 * @returns The constructed key.
	 */
	getKey(workflowName: string, type?: string, id?: string): string {
		let key = `${this.prefix}${C.QUEUE_CATEGORY_WF}:${workflowName}`;
		if (type) key += `:${type}`;
		if (id) key += `:${id}`;
		return key;
	}

	/**
	 * Get a key for a signal.
	 *
	 * @param signalName The name of the signal
	 * @param key The key of the signal
	 * @returns The constructed key for the signal.
	 */
	getSignalKey(signalName: string, key?: string): string {
		return `${this.prefix}${C.QUEUE_CATEGORY_SIGNAL}:${signalName}:${key}`;
	}

	/**
	 * Start the job processor for the given workflow.
	 */
	startJobProcessor(): void {
		if (this.running) return;
		this.running = true;
		this.runJobProcessor();
	}

	/**
	 * Stop the job processor for the given workflow.
	 */
	stopJobProcessor(): void {
		this.running = false;
	}

	/**
	 * Run the job processor.
	 */
	private async runJobProcessor(): Promise<void> {
		if (!this.wf) return; // No workflow set, can't process jobs

		while (this.running && this.connected) {
			try {
				const waitingKey = this.getKey(this.wf.name, C.QUEUE_WAITING);
				const waitingJobs = this.queues.get(waitingKey);

				if (waitingJobs && waitingJobs.size > 0) {
					const jobId = waitingJobs.values().next().value;
					waitingJobs.delete(jobId);

					// Move to active queue
					const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);
					if (!this.queues.has(activeKey)) {
						this.queues.set(activeKey, new Set());
					}
					this.queues.get(activeKey)!.add(jobId);

					// Add to running jobs
					this.wf.addRunningJob(jobId);

					// Process the job (don't await to allow parallel processing)
					setImmediate(() => this.processJob(jobId));
				} else {
					// No jobs to process, wait a bit
					await new Promise(resolve => setTimeout(resolve, 100));
				}
			} catch (err) {
				this.logger.error("Error in job processor:", err);
				await new Promise(resolve => setTimeout(resolve, 1000));
			}
		}
	}

	/**
	 * Process a job.
	 *
	 * @param jobId - The ID of the job to process.
	 */
	private async processJob(jobId: string): Promise<void> {
		try {
			const job = this.jobs.get(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
			if (!job) {
				this.logger.warn(`Job not found: ${jobId}`);
				return;
			}

			// Update job start time
			job.startedAt = Date.now();
			job.nodeID = this.broker.nodeID;

			// Add started event
			await this.addJobEvent(this.wf.name, jobId, {
				type: "started",
				ts: Date.now(),
				nodeID: this.broker.nodeID,
				taskType: "workflow"
			});

			// Send job event
			this.sendJobEvent(this.wf.name, jobId, "started");

			// Execute the job
			const result = await this.wf.callHandler(
				job,
				await this.getJobEvents(this.wf.name, jobId)
			);

			// Job completed successfully
			await this.moveToCompleted(job, result);
		} catch (err) {
			this.logger.error(`Error processing job ${jobId}:`, err);
			await this.moveToFailed(jobId, err);
		} finally {
			// Remove from running jobs
			this.wf.removeRunningJob(jobId);
		}
	}

	/**
	 * Move a job to the completed queue.
	 *
	 * @param job - The job object.
	 * @param result - The result of the job.
	 */
	async moveToCompleted(job: Job, result: unknown): Promise<void> {
		const jobId = job.id;
		const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);
		const completedKey = this.getKey(this.wf.name, C.QUEUE_COMPLETED);

		// Remove from active queue
		this.queues.get(activeKey)?.delete(jobId);

		// Add to completed queue
		if (!this.queues.has(completedKey)) {
			this.queues.set(completedKey, new Set());
		}
		this.queues.get(completedKey)!.add(jobId);

		// Update job
		job.finishedAt = Date.now();
		job.success = true;
		job.result = result;
		if (job.startedAt) {
			job.duration = job.finishedAt - job.startedAt;
		}

		// Save updated job
		this.jobs.set(this.getKey(this.wf.name, C.QUEUE_JOB, jobId), job);

		// Add completed event
		await this.addJobEvent(this.wf.name, jobId, {
			type: "completed",
			ts: Date.now(),
			nodeID: this.broker.nodeID,
			taskType: "workflow"
		});

		// Send job event
		this.sendJobEvent(this.wf.name, jobId, "completed");

		// Notify if someone is waiting for the result
		if (this.jobResultPromises.has(jobId)) {
			const promise = this.jobResultPromises.get(jobId)!;
			this.jobResultPromises.delete(jobId);
			promise.resolve(result);
		}
	}

	/**
	 * Move a job to the failed queue.
	 *
	 * @param job - The job object or job ID.
	 * @param err - The error that caused the failure.
	 */
	async moveToFailed(job: Job | string, err: Error | null): Promise<void> {
		const jobId = typeof job === "string" ? job : job.id;
		const jobObj =
			typeof job === "string"
				? this.jobs.get(this.getKey(this.wf.name, C.QUEUE_JOB, jobId))
				: job;

		if (!jobObj) {
			this.logger.warn(`Job not found for failure: ${jobId}`);
			return;
		}

		const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);
		const failedKey = this.getKey(this.wf.name, C.QUEUE_FAILED);

		// Remove from active queue
		this.queues.get(activeKey)?.delete(jobId);

		// Add to failed queue
		if (!this.queues.has(failedKey)) {
			this.queues.set(failedKey, new Set());
		}
		this.queues.get(failedKey)!.add(jobId);

		// Update job
		jobObj.finishedAt = Date.now();
		jobObj.success = false;
		jobObj.error = err ? this.broker.errorRegenerator.extractPlainError(err) : undefined;
		if (jobObj.startedAt) {
			jobObj.duration = jobObj.finishedAt - jobObj.startedAt;
		}

		// Save updated job
		this.jobs.set(this.getKey(this.wf.name, C.QUEUE_JOB, jobId), jobObj);

		// Add failed event
		await this.addJobEvent(this.wf.name, jobId, {
			type: "failed",
			ts: Date.now(),
			nodeID: this.broker.nodeID,
			taskType: "workflow",
			error: err ? this.broker.errorRegenerator.extractPlainError(err) : undefined
		});

		// Send job event
		this.sendJobEvent(this.wf.name, jobId, "failed");

		// Notify if someone is waiting for the result
		if (this.jobResultPromises.has(jobId)) {
			const promise = this.jobResultPromises.get(jobId)!;
			this.jobResultPromises.delete(jobId);
			promise.reject(err || new Error("Job failed"));
		}
	}

	/**
	 * Save state of a job.
	 *
	 * @param workflowName The name of workflow.
	 * @param jobId The ID of the job.
	 * @param state The state object to save.
	 * @returns Resolves when the state is saved.
	 */
	async saveJobState(workflowName: string, jobId: string, state: unknown): Promise<void> {
		const key = `${workflowName}:${jobId}`;
		this.jobStates.set(key, state);
	}

	/**
	 * Get state of a workflow run.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns Resolves with the state object or null if not found.
	 */
	async getState(workflowName: string, jobId: string): Promise<unknown> {
		const key = `${workflowName}:${jobId}`;
		return this.jobStates.get(key) || null;
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param signalName The name of the signal to trigger.
	 * @param key The key associated with the signal.
	 * @param payload The payload to send with the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	async triggerSignal(signalName: string, key?: string, payload?: unknown): Promise<void> {
		const signalKey = this.getSignalKey(signalName, key || C.SIGNAL_EMPTY_KEY);
		this.signals.set(signalKey, payload);

		// Notify waiting promises
		const pKey = signalName + ":" + (key || C.SIGNAL_EMPTY_KEY);
		const found = this.signalPromises.get(pKey);
		if (found) {
			this.signalPromises.delete(pKey);
			found.resolve(payload);
		}
	}

	/**
	 * Remove a named signal.
	 *
	 * @param signalName The name of the signal to trigger.
	 * @param key The key associated with the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	async removeSignal(signalName: string, key?: string): Promise<void> {
		const signalKey = this.getSignalKey(signalName, key || C.SIGNAL_EMPTY_KEY);
		this.signals.delete(signalKey);
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param signalName The name of the signal to wait for.
	 * @param key The key associated with the signal.
	 * @param opts Options for waiting for the signal.
	 * @returns The payload of the received signal.
	 */
	async waitForSignal<TSignalResult = unknown>(
		signalName: string,
		key?: string,
		opts?: SignalWaitOptions
	): Promise<TSignalResult> {
		const signalKey = this.getSignalKey(signalName, key || C.SIGNAL_EMPTY_KEY);

		// Check if signal already exists
		if (this.signals.has(signalKey)) {
			const payload = this.signals.get(signalKey);
			this.signals.delete(signalKey);
			return payload as TSignalResult;
		}

		// Create promise to wait for signal
		const pKey = signalName + ":" + (key || C.SIGNAL_EMPTY_KEY);

		return new Promise<TSignalResult>((resolve, reject) => {
			const promise: StoredPromise<TSignalResult> = {
				promise: null,
				resolve,
				reject
			};

			this.signalPromises.set(pKey, promise as StoredPromise<unknown>);

			// Set timeout if specified
			if (opts?.timeout) {
				setTimeout(() => {
					if (this.signalPromises.has(pKey)) {
						this.signalPromises.delete(pKey);
						reject(
							new WorkflowTimeoutError(
								signalName,
								key || "",
								parseDuration(opts.timeout)
							)
						);
					}
				}, parseDuration(opts.timeout));
			}
		});
	}

	/**
	 * Create a new job and push it to the waiting or delayed queue.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param job - The job.
	 * @param opts - Additional options for the job.
	 * @returns Resolves with the created job object.
	 */
	async newJob(workflowName: string, job: Job, _opts?: CreateJobOptions): Promise<Job> {
		// Store the job
		const jobKey = this.getKey(workflowName, C.QUEUE_JOB, job.id);
		this.jobs.set(jobKey, job);

		// Add to appropriate queue
		if (job.promoteAt && job.promoteAt > Date.now()) {
			// Add to delayed queue
			const delayedKey = workflowName;
			if (!this.delayedJobs.has(delayedKey)) {
				this.delayedJobs.set(delayedKey, []);
			}
			this.delayedJobs.get(delayedKey)!.push({ jobId: job.id, promoteAt: job.promoteAt });

			// Sort by promoteAt
			this.delayedJobs.get(delayedKey)!.sort((a, b) => a.promoteAt - b.promoteAt);
		} else {
			// Add to waiting queue
			const waitingKey = this.getKey(workflowName, C.QUEUE_WAITING);
			if (!this.queues.has(waitingKey)) {
				this.queues.set(waitingKey, new Set());
			}
			this.queues.get(waitingKey)!.add(job.id);
		}

		// Add created event
		await this.addJobEvent(workflowName, job.id, {
			type: "created",
			ts: Date.now(),
			nodeID: this.broker.nodeID,
			taskType: "workflow"
		});

		// Send job event
		this.sendJobEvent(workflowName, job.id, "created");

		// Add promise function to the job
		job.promise = async () => {
			// Get the Job to check the status
			const job2 = await this.getJob(workflowName, job.id, [
				"success",
				"finishedAt",
				"error",
				"result"
			]);

			if (job2 && job2.finishedAt) {
				if (job2.success) {
					return job2.result;
				} else {
					throw this.broker.errorRegenerator.restore(job2.error, {});
				}
			}

			// Check that Job promise is stored
			if (this.jobResultPromises.has(job.id)) {
				return this.jobResultPromises.get(job.id)!.promise;
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
	 * Reschedule a repeatable job based on its configuration.
	 *
	 * @param {string} workflowName - The name of workflow.
	 * @param {Job} job - The job object or job ID to reschedule.
	 * @returns Resolves when the job is rescheduled.
	 */
	async newRepeatChildJob(workflowName: string, job: Job): Promise<void> {
		// Create a new job based on the original
		const newJob = _.cloneDeep(job);
		newJob.id = Utils.generateToken();
		newJob.parent = job.id;
		newJob.repeatCounter = (job.repeatCounter || 0) + 1;

		// Reset job state
		for (const field of C.RERUN_REMOVABLE_FIELDS) {
			delete newJob[field];
		}

		await this.newJob(workflowName, newJob);
	}

	/**
	 * Get a job details.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @param fields - The fields to retrieve or true to retrieve all fields.
	 * @returns Resolves with the job object or null if not found.
	 */
	async getJob(
		workflowName: string,
		jobId: string,
		fields?: string[] | true
	): Promise<Job | null> {
		const job = this.jobs.get(this.getKey(workflowName, C.QUEUE_JOB, jobId));
		if (!job) return null;

		if (fields === true || !fields) {
			return _.cloneDeep(job);
		}

		// Return only specified fields
		const result = {} as Partial<Job>;
		for (const field of fields) {
			if (job[field] !== undefined) {
				result[field] = job[field];
			}
		}
		return result as Job;
	}

	/**
	 * Finish a parent job.
	 *
	 * @param workflowName
	 * @param jobId
	 */
	async finishParentJob(workflowName: string, jobId: string): Promise<void> {
		// This is typically used in more complex scenarios
		// For now, just mark it as completed
		const job = this.jobs.get(this.getKey(workflowName, C.QUEUE_JOB, jobId));
		if (job) {
			await this.moveToCompleted(job, null);
		}
	}

	/**
	 * Add a job event to storage.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {Partial<JobEvent>} event - The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	async addJobEvent(
		workflowName: string,
		jobId: string,
		event: Partial<JobEvent>
	): Promise<void> {
		const key = `${workflowName}:${jobId}`;
		if (!this.jobEvents.has(key)) {
			this.jobEvents.set(key, []);
		}

		const fullEvent: JobEvent = {
			type: event.type || "unknown",
			ts: event.ts || Date.now(),
			nodeID: event.nodeID || this.broker.nodeID,
			taskType: event.taskType || "workflow",
			...event
		};

		this.jobEvents.get(key)!.push(fullEvent);
	}

	/**
	 * Get job events from storage.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @returns Resolves with an array of job events.
	 */
	async getJobEvents(workflowName: string, jobId: string): Promise<JobEvent[]> {
		const key = `${workflowName}:${jobId}`;
		return _.cloneDeep(this.jobEvents.get(key) || []);
	}

	/**
	 * List all completed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listCompletedJobs(workflowName: string): Promise<ListFinishedJobResult[]> {
		const completedKey = this.getKey(workflowName, C.QUEUE_COMPLETED);
		const jobIds = this.queues.get(completedKey) || new Set();

		return Array.from(jobIds).map(id => {
			const job = this.jobs.get(this.getKey(workflowName, C.QUEUE_JOB, id));
			return {
				id,
				finishedAt: job?.finishedAt || 0
			};
		});
	}

	/**
	 * List all failed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listFailedJobs(workflowName: string): Promise<ListFinishedJobResult[]> {
		const failedKey = this.getKey(workflowName, C.QUEUE_FAILED);
		const jobIds = this.queues.get(failedKey) || new Set();

		return Array.from(jobIds).map(id => {
			const job = this.jobs.get(this.getKey(workflowName, C.QUEUE_JOB, id));
			return {
				id,
				finishedAt: job?.finishedAt || 0
			};
		});
	}

	/**
	 * List all delayed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listDelayedJobs(workflowName: string): Promise<ListDelayedJobResult[]> {
		const delayedJobs = this.delayedJobs.get(workflowName) || [];
		return delayedJobs.map(item => ({
			id: item.jobId,
			promoteAt: item.promoteAt
		}));
	}

	/**
	 * List all active job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listActiveJobs(workflowName: string): Promise<ListJobResult[]> {
		const activeKey = this.getKey(workflowName, C.QUEUE_ACTIVE);
		const jobIds = this.queues.get(activeKey) || new Set();

		return Array.from(jobIds).map(id => ({ id }));
	}

	/**
	 * List all waiting job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	async listWaitingJobs(workflowName: string): Promise<ListJobResult[]> {
		const waitingKey = this.getKey(workflowName, C.QUEUE_WAITING);
		const jobIds = this.queues.get(waitingKey) || new Set();

		return Array.from(jobIds).map(id => ({ id }));
	}

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns
	 */
	async cleanUp(workflowName?: string, jobId?: string): Promise<void> {
		if (jobId && workflowName) {
			// Remove specific job
			const jobKey = this.getKey(workflowName, C.QUEUE_JOB, jobId);
			this.jobs.delete(jobKey);
			this.jobEvents.delete(`${workflowName}:${jobId}`);
			this.jobStates.delete(`${workflowName}:${jobId}`);

			// Remove from all queues
			for (const [queueKey, jobSet] of this.queues.entries()) {
				if (queueKey.includes(workflowName)) {
					jobSet.delete(jobId);
				}
			}
		} else if (workflowName) {
			// Remove all jobs for workflow
			const prefix = this.getKey(workflowName);

			for (const [key] of this.jobs.entries()) {
				if (key.startsWith(prefix)) {
					this.jobs.delete(key);
				}
			}

			for (const [key] of this.jobEvents.entries()) {
				if (key.startsWith(workflowName + ":")) {
					this.jobEvents.delete(key);
				}
			}

			for (const [key] of this.jobStates.entries()) {
				if (key.startsWith(workflowName + ":")) {
					this.jobStates.delete(key);
				}
			}

			for (const [queueKey] of this.queues.entries()) {
				if (queueKey.includes(workflowName)) {
					this.queues.delete(queueKey);
				}
			}

			this.delayedJobs.delete(workflowName);
		} else {
			// Clear everything
			FakeAdapter.sharedJobs.clear();
			FakeAdapter.sharedJobEvents.clear();
			FakeAdapter.sharedJobStates.clear();
			FakeAdapter.sharedSignals.clear();
			FakeAdapter.sharedQueues.clear();
			FakeAdapter.sharedDelayedJobs.clear();
			FakeAdapter.sharedLocks.clear();
		}
	}

	/**
	 * Acquire a maintenance lock for a workflow.
	 *
	 * @param lockTime - The time to hold the lock in milliseconds.
	 * @param lockName - Lock name
	 * @returns Resolves with true if the lock is acquired, false otherwise.
	 */
	async lockMaintenance(lockTime: number, lockName?: string): Promise<boolean> {
		const key = lockName || "default";
		const now = Date.now();

		const existingLock = this.locks.get(key);
		if (existingLock && existingLock.lockedAt + existingLock.lockTime > now) {
			return false; // Lock is still active
		}

		this.locks.set(key, { lockedAt: now, lockTime });
		return true;
	}

	/**
	 * Release the maintenance lock for a workflow.
	 *
	 * @param lockName - Lock name
	 * @returns Resolves when the lock is released.
	 */
	async unlockMaintenance(lockName?: string): Promise<void> {
		const key = lockName || "default";
		this.locks.delete(key);
	}

	/**
	 * Process stalled jobs for a workflow and move them back to the waiting queue.
	 *
	 * @returns Resolves when stalled jobs are processed.
	 */
	async maintenanceStalledJobs(): Promise<void> {
		// Move jobs from active queue that have been stalled
		const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);
		const waitingKey = this.getKey(this.wf.name, C.QUEUE_WAITING);
		const stalledKey = this.getKey(this.wf.name, C.QUEUE_STALLED);

		const activeJobs = this.queues.get(activeKey);
		if (!activeJobs) return;

		const now = Date.now();
		const stalledJobIds: string[] = [];

		for (const jobId of activeJobs) {
			const job = this.jobs.get(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
			if (job && job.startedAt && now - job.startedAt > 30000) {
				// 30 seconds stall timeout
				stalledJobIds.push(jobId);
			}
		}

		// Move stalled jobs
		for (const jobId of stalledJobIds) {
			activeJobs.delete(jobId);

			// Add to stalled queue
			if (!this.queues.has(stalledKey)) {
				this.queues.set(stalledKey, new Set());
			}
			this.queues.get(stalledKey)!.add(jobId);

			// Add to waiting queue for retry
			if (!this.queues.has(waitingKey)) {
				this.queues.set(waitingKey, new Set());
			}
			this.queues.get(waitingKey)!.add(jobId);
		}
	}

	/**
	 * Check active jobs and if they timed out, move to failed jobs.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	async maintenanceActiveJobs(): Promise<void> {
		const activeKey = this.getKey(this.wf.name, C.QUEUE_ACTIVE);
		const activeJobs = this.queues.get(activeKey);
		if (!activeJobs) return;

		const now = Date.now();
		const timedOutJobIds: string[] = [];

		for (const jobId of activeJobs) {
			const job = this.jobs.get(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
			if (job && job.timeout && job.startedAt && now - job.startedAt > job.timeout) {
				timedOutJobIds.push(jobId);
			}
		}

		// Move timed out jobs to failed
		for (const jobId of timedOutJobIds) {
			await this.moveToFailed(jobId, new WorkflowTimeoutError(this.wf.name, jobId, 0));
		}
	}

	/**
	 * Remove old jobs from a specified queue based on their age.
	 *
	 * @param queueName - The name of the queue (e.g., completed, failed).
	 * @param retention - The age threshold in milliseconds for removing jobs.
	 * @returns Resolves when old jobs are removed.
	 */
	async maintenanceRemoveOldJobs(queueName: string, retention: number): Promise<void> {
		const queueKey = this.getKey(this.wf.name, queueName);
		const jobIds = this.queues.get(queueKey);
		if (!jobIds) return;

		const now = Date.now();
		const oldJobIds: string[] = [];

		for (const jobId of jobIds) {
			const job = this.jobs.get(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
			if (job && job.finishedAt && now - job.finishedAt > retention) {
				oldJobIds.push(jobId);
			}
		}

		// Remove old jobs
		for (const jobId of oldJobIds) {
			jobIds.delete(jobId);
			this.jobs.delete(this.getKey(this.wf.name, C.QUEUE_JOB, jobId));
			this.jobEvents.delete(`${this.wf.name}:${jobId}`);
			this.jobStates.delete(`${this.wf.name}:${jobId}`);
		}
	}

	/**
	 * Process delayed jobs for a workflow and move them to the waiting queue if ready.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	async maintenanceDelayedJobs(): Promise<void> {
		const delayedJobs = this.delayedJobs.get(this.wf.name);
		if (!delayedJobs || delayedJobs.length === 0) return;

		const now = Date.now();
		const readyJobs: { jobId: string; promoteAt: number }[] = [];

		// Find jobs ready to be promoted
		for (let i = 0; i < delayedJobs.length; i++) {
			if (delayedJobs[i].promoteAt <= now) {
				readyJobs.push(delayedJobs[i]);
			} else {
				break; // Since array is sorted, no more ready jobs
			}
		}

		// Remove ready jobs from delayed queue and add to waiting queue
		if (readyJobs.length > 0) {
			this.delayedJobs.set(this.wf.name, delayedJobs.slice(readyJobs.length));

			const waitingKey = this.getKey(this.wf.name, C.QUEUE_WAITING);
			if (!this.queues.has(waitingKey)) {
				this.queues.set(waitingKey, new Set());
			}

			for (const item of readyJobs) {
				this.queues.get(waitingKey)!.add(item.jobId);
			}
		}
	}

	/**
	 * Get the next delayed jobs maintenance time.
	 */
	async getNextDelayedJobTime(): Promise<number | null> {
		const delayedJobs = this.delayedJobs.get(this.wf.name);
		if (!delayedJobs || delayedJobs.length === 0) return null;

		return delayedJobs[0].promoteAt;
	}

	/**
	 * Dump all data for all workflows to JSON files.
	 *
	 * @param folder - The folder to save the dump files.
	 * @param wfNames - The names of the workflows to dump.
	 */
	async dumpWorkflows(folder: string, wfNames: string[]): Promise<void> {
		for (const wfName of wfNames) {
			await this.dumpWorkflow(wfName, folder);
		}
	}

	/**
	 * Dump all data for a workflow to a JSON file.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param folder - The folder to save the dump files.
	 */
	async dumpWorkflow(workflowName: string, folder: string): Promise<void> {
		const dump = {
			jobs: {} as Record<string, Job>,
			jobEvents: {} as Record<string, JobEvent[]>,
			jobStates: {} as Record<string, unknown>,
			queues: {} as Record<string, string[]>,
			delayedJobs: this.delayedJobs.get(workflowName) || []
		};

		// Collect jobs
		const prefix = this.getKey(workflowName);
		for (const [key, job] of this.jobs.entries()) {
			if (key.startsWith(prefix)) {
				dump.jobs[key] = job;
			}
		}

		// Collect job events
		for (const [key, events] of this.jobEvents.entries()) {
			if (key.startsWith(workflowName + ":")) {
				dump.jobEvents[key] = events;
			}
		}

		// Collect job states
		for (const [key, state] of this.jobStates.entries()) {
			if (key.startsWith(workflowName + ":")) {
				dump.jobStates[key] = state;
			}
		}

		// Collect queues
		for (const [key, jobSet] of this.queues.entries()) {
			if (key.includes(workflowName)) {
				dump.queues[key] = Array.from(jobSet);
			}
		}

		// Write to file
		const fs = await import("node:fs/promises");
		const path = await import("node:path");
		const filename = path.join(folder, `${workflowName.replace(/[^a-zA-Z0-9]/g, "_")}.json`);
		await fs.writeFile(filename, JSON.stringify(dump, null, 2));
	}
}
