// TypeScript type definitions for the Moleculer Workflow project

import { ServiceBroker, LoggerInstance, Context, PlainMoleculerError } from "moleculer";
import { ClusterOptions, RedisOptions } from "ioredis";

import BaseAdapter, { BaseDefaultOptions } from "./adapters/base";
import RedisAdapter, { RedisAdapterOptions } from "./adapters/redis";

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
	waitForSignal: (signalName: string, key?: string, opts?: SignalWaitOptions) => Promise<unknown>;
	task: (name: string, fn: () => Promise<unknown>) => Promise<unknown>;
}

export interface WorkflowServiceBroker {
	run: (workflowName: string, payload: unknown, opts?: unknown) => Promise<unknown>;
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

export interface RedisAdapterOptions extends BaseDefaultOptions {
	redis?:
		| RedisOptions
		| { url: string }
		| { cluster: { nodes: string[]; clusterOptions?: ClusterOptions } };
	prefix?: string;
	serializer?: string;
	drainDelay?: number;
}
