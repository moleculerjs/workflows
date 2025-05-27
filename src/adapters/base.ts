/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

import _ from "lodash";
import { WorkflowError } from "../errors.ts";
import type { ServiceBroker, LoggerInstance as Logger } from "moleculer";
import type {
	Job,
	JobEvent,
	WorkflowsMiddlewareOptions,
	SignalWaitOptions,
	CreateJobOptions
} from "../types.ts";
import type Workflow from "../workflow.ts";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface BaseDefaultOptions extends Record<string, unknown> {}

export type ListJobResult = { id: string };
export type ListFinishedJobResult = { id: string; finishedAt: number };
export type ListDelayedJobResult = { id: string; promoteAt: number };

/**
 * Base adapter class
 */
export default abstract class BaseAdapter {
	opts: BaseDefaultOptions;
	connected: boolean;
	wf?: Workflow;
	broker?: ServiceBroker;
	logger?: Logger;
	mwOpts?: WorkflowsMiddlewareOptions;

	/**
	 * Constructor of adapter
	 * @param opts
	 */
	constructor(opts?: BaseDefaultOptions) {
		this.opts = _.defaultsDeep({}, opts, {});
		this.connected = false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param wf
	 * @param broker
	 * @param logger
	 * @param mwOpts - Middleware options.
	 */
	init(wf, broker, logger, mwOpts) {
		/** @type {Workflow} */
		this.wf = wf;

		this.broker = broker;
		this.logger = logger;
		this.mwOpts = mwOpts;
	}

	/**
	 * Log a message with the given level.
	 *
	 * @param level
	 * @param workflowName
	 * @param jobId
	 * @param msg
	 * @param args
	 */
	log(level, workflowName, jobId, msg, ...args) {
		if (this.logger) {
			const wfJobName = jobId ? `${workflowName}:${jobId}` : workflowName;
			this.logger[level](`[${wfJobName}] ${msg}`, ...args);
		}
	}

	/**
	 * Connect to the adapter.
	 */
	abstract connect();

	/**
	 * Close the adapter.
	 */
	abstract disconnect();

	/**
	 * Start the job processor for the given workflow.
	 */
	abstract startJobProcessor();

	/**
	 * Stop the job processor for the given workflow.
	 */
	abstract stopJobProcessor();

	/**
	 * Save state of a job.
	 *
	 * @param workflowName The name of workflow.
	 * @param jobId The ID of the job.
	 * @param state The state object to save.
	 * @returns Resolves when the state is saved.
	 */
	abstract saveJobState(workflowName: string, jobId: string, state: unknown): Promise<void>;

	/**
	 * Get state of a workflow run.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns Resolves with the state object or null if not found.
	 */
	abstract getState(workflowName: string, jobId: string): Promise<unknown>;

	/**
	 * Trigger a named signal.
	 *
	 * @param signalName The name of the signal to trigger.
	 * @param key The key associated with the signal.
	 * @param payload The payload to send with the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	abstract triggerSignal(signalName: string, key?: string, payload?: unknown): Promise<void>;

	/**
	 * Remove a named signal.
	 *
	 * @param signalName The name of the signal to trigger.
	 * @param key The key associated with the signal.
	 * @returns Resolves when the signal is triggered.
	 */
	abstract removeSignal(signalName: string, key?: string): Promise<void>;

	/**
	 * Wait for a named signal.
	 *
	 * @param signalName The name of the signal to wait for.
	 * @param key The key associated with the signal.
	 * @param opts Options for waiting for the signal.
	 * @returns The payload of the received signal.
	 */
	abstract waitForSignal<TSignalResult = unknown>(
		signalName: string,
		key?: string,
		opts?: SignalWaitOptions
	): Promise<TSignalResult>;

	/**
	 * Create a new job and push it to the waiting or delayed queue.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param job - The job.
	 * @param opts - Additional options for the job.
	 * @returns Resolves with the created job object.
	 */
	abstract newJob(workflowName: string, job: Job, opts?: CreateJobOptions): Promise<Job>;

	/**
	 * Reschedule a repeatable job based on its configuration.
	 *
	 * @param {string} workflowName - The name of workflow.
	 * @param {Job} job - The job object or job ID to reschedule.
	 * @returns Resolves when the job is rescheduled.
	 */
	abstract newRepeatChildJob(workflowName: string, job: Job): Promise<void>;

	/**
	 * Get a job details.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @param fields - The fields to retrieve or true to retrieve all fields.
	 * @returns Resolves with the job object or null if not found.
	 */
	abstract getJob(
		workflowName: string,
		jobId: string,
		fields?: string[] | true
	): Promise<Job | null>;

	/**
	 * Finish a parent job.
	 *
	 * @param workflowName
	 * @param jobId
	 */
	abstract finishParentJob(workflowName: string, jobId: string): Promise<void>;

	/**
	 * Add a job event to Redis.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {Partial<JobEvent>} event - The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	abstract addJobEvent(
		workflowName: string,
		jobId: string,
		event: Partial<JobEvent>
	): Promise<void>;

	/**
	 * Get job events from Redis.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param jobId - The ID of the job.
	 * @returns Resolves with an array of job events.
	 */
	abstract getJobEvents(workflowName: string, jobId: string): Promise<object[]>;

	/**
	 * List all completed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	abstract listCompletedJobs(workflowName: string): Promise<ListFinishedJobResult[]>;

	/**
	 * List all failed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	abstract listFailedJobs(workflowName: string): Promise<ListFinishedJobResult[]>;

	/**
	 * List all delayed job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	abstract listDelayedJobs(workflowName: string): Promise<ListDelayedJobResult[]>;

	/**
	 * List all active job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	abstract listActiveJobs(workflowName: string): Promise<ListJobResult[]>;

	/**
	 * List all waiting job IDs for a workflow.
	 * @param workflowName
	 * @returns
	 */
	abstract listWaitingJobs(workflowName: string): Promise<ListJobResult[]>;

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 *
	 * @param workflowName
	 * @param jobId
	 * @returns
	 */
	abstract cleanUp(workflowName?: string, jobId?: string): Promise<void>;

	/**
	 * Acquire a maintenance lock for a workflow.
	 *
	 * @param lockTime - The time to hold the lock in milliseconds.
	 * @param lockName - Lock name
	 * @returns Resolves with true if the lock is acquired, false otherwise.
	 */
	abstract lockMaintenance(lockTime: number, lockName?: string): Promise<boolean>;

	/**
	 * Release the maintenance lock for a workflow.
	 *
	 * @param lockName - Lock name
	 * @returns Resolves when the lock is released.
	 */
	abstract unlockMaintenance(lockName?: string): Promise<void>;

	/**
	 * Process stalled jobs for a workflow and move them back to the waiting queue.
	 *
	 * @returns Resolves when stalled jobs are processed.
	 */
	abstract maintenanceStalledJobs();

	/**
	 * Check active jobs and if they timed out, move to failed jobs.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	abstract maintenanceActiveJobs();

	/**
	 * Remove old jobs from a specified queue based on their age.
	 *
	 * @param queueName - The name of the queue (e.g., completed, failed).
	 * @param retention - The age threshold in milliseconds for removing jobs.
	 * @returns Resolves when old jobs are removed.
	 */
	abstract maintenanceRemoveOldJobs(queueName: string, retention: number): Promise<void>;

	/**
	 * Process delayed jobs for a workflow and move them to the waiting queue if ready.
	 *
	 * @returns Resolves when delayed jobs are processed.
	 */
	abstract maintenanceDelayedJobs(): Promise<void>;

	/**
	 * Get the next delayed jobs maintenance time.
	 */
	abstract getNextDelayedJobTime(): Promise<number | null>;

	/**
	 * Dump all Redis data for all workflows to JSON files.
	 *
	 * @param folder - The folder to save the dump files.
	 * @param wfNames - The names of the workflows to dump.
	 */
	abstract dumpWorkflows(folder: string, wfNames: string[]): Promise<string[]>;

	/**
	 * Dump all Redis data for a workflow to a JSON file.
	 *
	 * @param workflowName - The name of the workflow.
	 * @param folder - The folder to save the dump files.
	 * @returns Path to the dump file.
	 */
	abstract dumpWorkflow(workflowName: string, folder: string): Promise<string>;

	/**
	 * Send entity lifecycle events
	 *
	 * @param workflowName
	 * @param jobId
	 * @param type
	 */
	sendJobEvent(workflowName: string, jobId: string, type) {
		if (this.mwOpts?.jobEventType) {
			const eventName = `job.${workflowName}.${type}`;

			const payload = {
				type,
				workflow: workflowName,
				job: jobId
			};

			this.broker![this.mwOpts.jobEventType](eventName, payload);
		}
	}

	/**
	 * Check if the job ID is valid.
	 *
	 * @param jobId
	 */
	checkJobId(jobId: string) {
		const re = /^[a-zA-Z0-9_.-]+$/;
		if (!re.test(jobId)) {
			throw new WorkflowError(
				`Invalid job ID '${jobId}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_JOB_ID",
				{
					jobId
				}
			);
		}

		return jobId;
	}
}
