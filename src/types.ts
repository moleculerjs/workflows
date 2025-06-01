/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

import { Context, Errors } from "moleculer";

import type { ResolvableAdapterType } from "./adapters/index.ts";
import BaseAdapter, {
	ListDelayedJobResult,
	ListFinishedJobResult,
	ListJobResult
} from "./adapters/base.ts";
import RedisAdapter from "./adapters/redis.ts";

/**
 * Options for the Workflows middleware
 */
export interface WorkflowsMiddlewareOptions {
	adapter?: ResolvableAdapterType;
	schemaProperty?: string;
	workflowHandlerTrigger?: string;
	jobEventType?: string;
	jobIdCollision?: "reject" | "skip" | "rerun";

	signalExpiration?: string;
	maintenanceTime?: number;
	lockExpiration?: number;
	tracing?: boolean;
}

export type WorkflowHandler = (ctx: Context) => Promise<unknown | void>;

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
	error?: Errors.PlainMoleculerError;
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
	error?: Errors.PlainMoleculerError;
	result?: unknown;
	duration?: number;

	promise?: () => Promise<unknown>;
}

export interface WorkflowContextProps {
	name: string;
	jobId: string;
	retryAttempts?: number;
	retries?: number;
	timeout?: string | number;

	sleep: (duration: number | string) => Promise<void>;
	setState: (state: unknown) => Promise<void>;
	waitForSignal: (signalName: string, key?: string, opts?: SignalWaitOptions) => Promise<unknown>;
	task: (name: string, fn: () => Promise<unknown>) => Promise<unknown>;
}

export interface WorkflowServiceBrokerMethods {
	run: (workflowName: string, payload?: unknown, opts?: unknown) => Promise<Job>;
	remove: (workflowName: string, jobId: string) => Promise<void>;
	triggerSignal: (signalName: string, key?: unknown, payload?: unknown) => Promise<void>;
	removeSignal: (signalName: string, key?: unknown) => Promise<void>;
	getAdapter: () => Promise<BaseAdapter>;
	getState: (workflowName: string, jobId: string) => Promise<unknown>;
	get: (workflowName: string, jobId: string) => Promise<Job | null>;
	getEvents: (workflowName: string, jobId: string) => Promise<JobEvent[] | null>;
	listCompletedJobs: (workflowName: string) => Promise<ListFinishedJobResult[]>;
	listFailedJobs: (workflowName: string) => Promise<ListFinishedJobResult[]>;
	listDelayedJobs: (workflowName: string) => Promise<ListDelayedJobResult[]>;
	listActiveJobs: (workflowName: string) => Promise<ListJobResult[]>;
	listWaitingJobs: (workflowName: string) => Promise<ListJobResult[]>;
	cleanUp: (workflowName: string) => Promise<void>;

	adapter: RedisAdapter | BaseAdapter;
}
