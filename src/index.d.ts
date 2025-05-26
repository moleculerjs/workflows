// TypeScript type definitions for the Moleculer Workflow project

import { ServiceBroker, LoggerInstance, Context, PlainMoleculerError } from "moleculer";
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
	tracing?: boolean;
}

export type WorkflowHandler = (ctx: WorkflowContext) => Promise<unknown>;

export interface CreateJobOptions {
	jobId?: string;
	retries?: number;
	delay?: number;
	timeout?: number;
	repeat?: JobRepeat;
	isCustomJobId?: boolean;
}

export interface SignalWaitOptions {
	timeout?: number | string;
}

export type JobRepeat = {
	endDate?: number;
	cron?: string;
	tz?: string;
	limit?: number;
};

export interface JobEvent {
	type: string;
	ts: number;
	nodeID: string;
	taskId?: number;
	taskType: string;
	duration?: number;
	result?: unknown;
	error?: PlainMoleculerError;
}

export interface Job {
	id: string;
	parent?: string;
	payload?: unknown;

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
	error?: PlainMoleculerError;
	result?: unknown;
	duration?: number;

	promise?: () => Promise<unknown>;
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
	tracing?: boolean;
	maxStalledCount?: number;
}

export interface WorkflowSchema extends WorkflowOptions {
	fullName?: string;
	handler: (ctx: WorkflowContext) => Promise<unknown>;
}

export interface WorkflowContext extends Context {
	wf: WorkflowContextProps;
}

export interface WorkflowContextProps {
	name: string;
	jobId: string;
	retryAttempts?: number;
	retries?: number;
	timeout?: string | number;

	sleep: (duration: number) => Promise<void>;
	setState: (state: unknown) => Promise<void>;
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

export class RedisAdapter extends BaseAdapter {
	opts: RedisAdapterOptions;
	constructor(opts?: RedisAdapterOptions);

	getKey: (name: string, type?: string, id?: string) => string;
}
