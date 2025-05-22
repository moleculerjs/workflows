// TypeScript type definitions for the Moleculer Workflow project

import { ServiceBroker, LoggerInstance, Context } from "moleculer";
import { Cluster, Redis, RedisOptions } from "ioredis";

/**
 * Options for the Workflows middleware
 */
export interface WorkflowsMiddlewareOptions {
    adapter: string | BaseAdapter | RedisAdapterOptions;
    schemaProperty?: string;
    workflowHandlerTrigger?: string;
    jobEventType?: string;
	jobIdCollision: "reject" | "skip" | "rerun";

    signalExpiration?: string;
    maintenanceTime?: number;
    lockExpiration?: number;
}

export type WorkflowHandler = (ctx: WorkflowContext) => Promise<unknown>;

export interface CreateJobOptions {
	jobId?: string;
	retries?: number;
	delay?: number;
	timeout?: number;
	repeat?: JobRepeat;
}

export interface SignalWaitOptions {
	timout?: number|string;
}

export type JobRepeat = {
	endDate?: number;
	cron?: string;
	tz?: string;
	limit?: number;
}

export interface JobEvent {
	type: string;
	ts: number;
	nodeID: string;
	taskId?: number;
	taskType: string;
	duration?: number;
	result?: unknown;
	error?: Record<string, any>;
}

export interface Job {
	id: string;
	parent?: string;
	payload?: Record<string, any>;

	delay?: number;
	timeout?: number;
	retries?: number;
	retryAttempts?: number;

	repeat?: JobRepeat;
	repeatCounter?: number;

	createdAt?: number;
	promoteAt?: number;

	startedAt?: number;
	stalledCounter?: number;
	state?: unknown;

	success?: boolean;
	finishedAt?: number;
	nodeID?: string;
	error?: Record<string, any>;
	result?: unknown;
	duration?: number;

	promise?: () => Promise<any>;
}

export class Workflow {
	opts: WorkflowSchema;
	name: string;
    handler: WorkflowHandler;

	sendJobEvent: (workflowName: string, jobId: string, type: string) => void;
	callHandler: (job: Job, events: JobEvent[]) => Promise<unknown>;
	setNextDelayedMaintenance: (time: number) => Promise<void>;
	addRunningJob: (jobId: string) => void;
	removeRunningJob: (jobId: string) => void;
	getNumberOfActiveJobs: () => number;
}

export interface WorkflowOptions {
    name?: string;
    timeout?: string | number;
    retention?: string | number;
    concurrency?: number;
    retryPolicy?: {
        retries?: number;
        delay?: number;
        maxDelay?: number;
        factor?: number;
    };

	removeOnCompleted?: boolean;
	removeOnFailed?: boolean;

    params?: Record<string, any>;
	maxStalledCount?: number;
}

export interface WorkflowSchema extends WorkflowOptions {
    fullName?: string;
    handler: (ctx: WorkflowContext) => Promise<unknown>;
}

export interface WorkflowContext {
	wf: WorkflowContextProps;
}

export interface WorkflowContextProps {
	name: string;
	jobId: string;
	retryAttempts?: number;
	retries?: number;
	timeout?: string|number;

	sleep: (duration: number) => Promise<void>;
	setState: (state: any) => Promise<void>;
	waitForSignal: (signalName: string, key?: any, opts?: any) => Promise<unknown>;
	task: (name: string, fn: () => Promise<unknown>) => Promise<unknown>;
}

export interface WorkflowServiceBroker {
	run: (workflowName: string, payload: any, opts?: any) => Promise<unknown>;
	remove: (workflowName: string, jobId: string) => Promise<void>;
	triggerSignal: (signalName: string, key?: unknown, payload?: unknown) => Promise<void>;
	removeSignal: (signalName: string, key?: unknown) => Promise<void>;
	getState: (workflowName: string, jobId: string) => Promise<unknown>;
	get: (workflowName: string, jobId: string) => Promise<unknown>;
	getEvents: (workflowName: string, jobId: string) => Promise<unknown>;
	listCompletedJobs: (workflowName: string) => Promise<unknown>;
	listFailedJobs: (workflowName: string) => Promise<unknown>;
	listDelayedJobs: (workflowName: string) => Promise<unknown>;
	listActiveJobs: (workflowName: string) => Promise<unknown>;
	listWaitingJobs: (workflowName: string) => Promise<unknown>;
	cleanUp: (workflowName: string) => Promise<void>;

	adapter: RedisAdapter | BaseAdapter;
}

export interface BaseDefaultOptions {}

export interface RedisAdapterOptions extends BaseDefaultOptions {
    redis?: RedisOptions | { url: string } | { cluster: { nodes: string[]; clusterOptions?: any } };
    prefix?: string;
    serializer?: string;
    drainDelay?: number;
}

export class BaseAdapter {
	connected: boolean;

    constructor(opts?: BaseDefaultOptions);
    init(wf: Workflow|null, broker: ServiceBroker, logger: LoggerInstance, mwOpts: WorkflowsMiddlewareOptions): void;
    connect(): Promise<void>;
    disconnect(): Promise<void>;

	startJobProcessor(): Promise<void>;
	stopJobProcessor(): Promise<void>;
    createJob(workflowName: string, payload: any, opts?: any): Promise<unknown>;
    cleanUp(workflowName?: string, jobId?: string): Promise<void>;

	addJobEvent(workflowName: string, jobId: string, event: JobEvent): Promise<void>;
	saveJobState(workflowName: string, jobId: string, state: unknown): Promise<void>;

	waitForSignal(signalName: string, key?: string, opts?: SignalWaitOptions): Promise<unknown>;

	lockMaintenance(lockTime: number, lockName?: string): Promise<boolean>;
	unlockMaintenance(lockName?: string): Promise<void>;

	maintenanceStalledJobs(): Promise<void>;
	maintenanceDelayedJobs(): Promise<void>;
	maintenanceActiveJobs(): Promise<void>;
	maintenanceRemoveOldJobs(queueName: string, retention: number): Promise<void>;

	getNextDelayedJobTime(): Promise<number|null>;
}

export class RedisAdapter extends BaseAdapter {
	opts: RedisAdapterOptions;
    constructor(opts?: RedisAdapterOptions);

	getKey: (name: string, type?: string, id?: string) => string;
}
