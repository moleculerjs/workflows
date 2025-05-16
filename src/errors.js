/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const { MoleculerError, MoleculerRetryableError } = require("moleculer").Errors;

class WorkflowError extends MoleculerError {
	constructor(message, code, type, data) {
		super(message, code || 500, type || "WORKFLOW_ERROR", data);
	}
}

class WorkflowRetryableError extends MoleculerRetryableError {
	constructor(message, code, type, data) {
		super(message, code || 500, type || "WORKFLOW_ERROR", data);
	}
}

class WorkflowTimeoutError extends WorkflowRetryableError {
	constructor(workflow, jobId, timeout) {
		super("Job timed out", 500, "WORKFLOW_JOB_TIMEOUT", { workflow, jobId, timeout });
	}
}

class WorkflowSignalTimeoutError extends WorkflowRetryableError {
	constructor(signal, key, timeout) {
		super("Signal timed out", 500, "WORKFLOW_SIGNAL_TIMEOUT", { signal, key, timeout });
	}
}

class WorkflowTaskMismatchError extends WorkflowError {
	constructor(taskId, expected, actual) {
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

class WorkflowAlreadyLocked extends WorkflowRetryableError {
	constructor(jobId) {
		super("Job is already locked", 500, "WORKFLOW_ALREADY_LOCKED", { jobId });
	}
}

module.exports = {
	WorkflowError,
	WorkflowTimeoutError,
	WorkflowSignalTimeoutError,
	WorkflowAlreadyLocked,
	WorkflowTaskMismatchError
};
