// TypeScript type definitions for the Moleculer Workflow project

import { ServiceBroker, LoggerInstance } from "moleculer";
import { Cluster, Redis, RedisOptions } from "ioredis";

export interface Workflow {
    name: string;
    handler: (ctx: WorkflowContext) => Promise<any>;
}

export interface WorkflowContext {
    wf: {
        name: string;
        jobId: string;
    };
    call: (action: string, params?: any) => Promise<any>;
    mcall: (calls: Record<string, any>) => Promise<any>;
    broadcast: (event: string, payload?: any) => void;
    emit: (event: string, payload?: any) => void;
    wf: {
        sleep: (duration: number) => Promise<void>;
        setState: (state: any) => Promise<void>;
        waitForSignal: (signalName: string, key: any, opts?: any) => Promise<any>;
        run: (name: string, fn: () => Promise<any>) => Promise<any>;
    };
}

export interface BaseDefaultOptions {
    prefix?: string;
    serializer: string;
    signalExpiration: string;
    maintenanceTime: number;
    removeCompletedAfter: string;
    removeFailedAfter: string;
    backoff: "fixed" | "exponential" | ((retryAttempts: number) => number);
    backoffDelay: number;
}

export interface RedisAdapterOptions extends BaseDefaultOptions {
    redis: RedisOptions | { url: string } | { cluster: { nodes: string[]; clusterOptions?: any } };
    drainDelay: number;
    lockDuration: number;
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
    connect(): Promise<void>;
    destroy(): Promise<void>;
    createJob(workflowName: string, payload: any, opts?: any): Promise<any>;
    cleanUp(workflowName?: string, jobId?: string): Promise<void>;
}
