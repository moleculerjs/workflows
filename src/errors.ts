/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

import { Errors } from "moleculer";
const { MoleculerError, MoleculerRetryableError } = Errors;

export class WorkflowError extends MoleculerError {
	constructor(message: string, code?: number, type?: string, data?: object) {
		super(message, code ?? 500, type ?? "WORKFLOW_ERROR", data);
	}
}

export class WorkflowRetryableError extends MoleculerRetryableError {
	constructor(message: string, code?: number, type?: string, data?: object) {
		super(message, code || 500, type || "WORKFLOW_ERROR", data);
	}
}

export class WorkflowTimeoutError extends WorkflowRetryableError {
	constructor(workflow: string, jobId: string, timeout: number) {
		super("Job timed out", 500, "WORKFLOW_JOB_TIMEOUT", { workflow, jobId, timeout });
	}
}

export class WorkflowSignalTimeoutError extends WorkflowRetryableError {
	constructor(signal: string, key: string, timeout: number | string) {
		super("Signal timed out", 500, "WORKFLOW_SIGNAL_TIMEOUT", { signal, key, timeout });
	}
}

export class WorkflowTaskMismatchError extends WorkflowError {
	constructor(taskId: number, expected: string, actual: string) {
		super(
			`Workflow task mismatch at replaying. Expected '${expected}' but got '${actual}'.`,
			500,
			"WORKFLOW_TASK_MISMATCH",
			{
				taskId,
				expected,
				actual
			}
		);
	}
}

export class WorkflowAlreadyLocked extends WorkflowRetryableError {
	constructor(jobId: string) {
		super("Job is already locked", 500, "WORKFLOW_ALREADY_LOCKED", { jobId });
	}
}

export class WorkflowMaximumStalled extends WorkflowRetryableError {
	constructor(jobId: string, maxStalledCount: number) {
		super("Job stalled too many times.", 500, "WORKFLOW_MAXIMUM_STALLED", {
			jobId,
			maxStalledCount
		});
	}
}
