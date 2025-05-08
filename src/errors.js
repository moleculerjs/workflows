/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const { MoleculerError } = require("moleculer").Errors;

class WorkflowError extends MoleculerError {
	constructor(message, code, type, data) {
		super(message, code || 500, type || "WORKFLOW_ERROR", data);
	}
}

class WorkflowTimeoutError extends WorkflowError {
	constructor(message, type, data) {
		super(message, 500, type || "WORKFLOW_TIMEOUT_ERROR", data);
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

class WorkflowAlreadyLocked extends WorkflowError {
	constructor(jobId) {
		super("Job is already locked", 500, "WORKFLOW_ALREADY_LOCKED", { jobId });
	}
}

module.exports = {
	WorkflowError,
	WorkflowTimeoutError,
	WorkflowAlreadyLocked,
	WorkflowTaskMismatchError
};
