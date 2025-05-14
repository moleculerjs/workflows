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
}

export interface Workflow {
    name?: string;
    timeout?: string | number;
    retention?: string | number;
    concurrency?: number;
    retries?: number;
    retryPolicy?: {
        enabled?: boolean;
        retries?: number;
        delay?: number;
        maxDelay?: number;
        factor?: number;
        backoff?: "fixed" | "exponential" | ((retryAttempts: number) => number);
    };
    backoff?: "fixed" | "exponential" | ((retryAttempts: number) => number);
    backoffDelay?: number;

    signalExpiration?: string;
    maintenanceTime?: number;
    lockExpiration?: number;

    params?: Record<string, any>;
    handler: (ctx: WorkflowContext) => Promise<any>;
}

export interface WorkflowContext extends Context {
    wf: {
        name: string;
        jobId: string;
        sleep: (duration: number) => Promise<void>;
        setState: (state: any) => Promise<void>;
        waitForSignal: (signalName: string, key: any, opts?: any) => Promise<any>;
        run: (name: string, fn: () => Promise<any>) => Promise<any>;
    };
}

export interface BaseDefaultOptions {
    prefix?: string;
    serializer?: string;
}

export interface RedisAdapterOptions extends BaseDefaultOptions {
    redis: RedisOptions | { url: string } | { cluster: { nodes: string[]; clusterOptions?: any } };
    drainDelay?: number;
}

export class BaseAdapter {
    constructor(opts?: BaseDefaultOptions);
    init(broker: ServiceBroker, logger: LoggerInstance): void;
    connect(): Promise<void>;
    destroy(): Promise<void>;
    createJob(workflowName: string, payload: any, opts?: any): Promise<any>;
    cleanUp(workflowName?: string, jobId?: string): Promise<void>;
}

export class RedisAdapter extends BaseAdapter {
    constructor(opts?: RedisAdapterOptions);
}
