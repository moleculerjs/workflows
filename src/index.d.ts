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

    signalExpiration?: string;
    maintenanceTime?: number;
    lockExpiration?: number;
}

export type WorkflowHandler = (ctx: WorkflowContext) => Promise<unknown>;

export class Workflow {
	opts: WorkflowSchema;
	name: string;
    handler: WorkflowHandler;
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

    params?: Record<string, any>;
	maxStalledCount?: number;
}

export interface WorkflowSchema extends WorkflowOptions {
    fullName?: string;
    handler: (ctx: WorkflowContext) => Promise<unknown>;
}

export interface WorkflowContext {
	name: string;
	jobId: string;
	sleep: (duration: number) => Promise<void>;
	setState: (state: any) => Promise<void>;
	waitForSignal: (signalName: string, key?: any, opts?: any) => Promise<unknown>;
	run: (name: string, fn: () => Promise<unknown>) => Promise<unknown>;
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

export interface BaseDefaultOptions {
    serializer?: string;
}

export interface RedisAdapterOptions extends BaseDefaultOptions {
    redis: RedisOptions | { url: string } | { cluster: { nodes: string[]; clusterOptions?: any } };
    prefix?: string;
    drainDelay?: number;
}

export class BaseAdapter {
    constructor(opts?: BaseDefaultOptions);
    init(broker: ServiceBroker, logger: LoggerInstance): void;
    connect(): Promise<void>;
    destroy(): Promise<void>;
    createJob(workflowName: string, payload: any, opts?: any): Promise<unknown>;
    cleanUp(workflowName?: string, jobId?: string): Promise<void>;
}

export class RedisAdapter extends BaseAdapter {
    constructor(opts?: RedisAdapterOptions);
}
