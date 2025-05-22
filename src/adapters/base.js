/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

/* eslint-disable no-unused-vars */

"use strict";

const _ = require("lodash");
const C = require("../constants");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").Service} Service Moleculer Service definition
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("../index.d.ts").Workflow} Workflow Workflow definition
 * @typedef {import("../index.d.ts").WorkflowsMiddlewareOptions} WorkflowsMiddlewareOptions Workflow middleware options
 */

/**
 * Base adapter class
 *
 * @class BaseAdapter
 * @typedef {import("../index.d.ts").BaseDefaultOptions} BaseDefaultOptions
 */
class BaseAdapter {
	/**
	 * Constructor of adapter
	 * @param {BaseDefaultOptions=} opts
	 */
	constructor(opts) {
		/** @type {BaseDefaultOptions} */
		this.opts = _.defaultsDeep({}, opts, {});
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {Workflow} wf
	 * @param {ServiceBroker} broker
	 * @param {Logger} logger
	 * @param {WorkflowsMiddlewareOptions} mwOpts - Middleware options.
	 */
	init(wf, broker, logger, mwOpts) {
		/** @type {Workflow} */
		this.wf = wf;

		this.broker = broker;
		this.logger = logger;
		this.mwOpts = mwOpts;
	}

	/**
	 * Log a message with the given level.
	 *
	 * @param {*} level
	 * @param {*} workflowName
	 * @param {*} jobId
	 * @param {*} msg
	 * @param  {...any} args
	 */
	log(level, workflowName, jobId, msg, ...args) {
		if (this.logger) {
			const wfJobName = jobId ? `${workflowName}:${jobId}` : workflowName;
			this.logger[level](`[${wfJobName}] ${msg}`, ...args);
		}
	}

	/**
	 * Connect to the adapter.
	 *
	 * @returns {Promise<void>}
	 */
	async connect() {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Close the adapter.
	 */
	disconnect() {
		// TODO: Implement adapter close logic
	}

	/**
	 * Start the job processor for the given workflow.
	 */
	startJobProcessor(/*workflow*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Stop the job processor for the given workflow.
	 */
	stopJobProcessor(/*workflow*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Create a job
	 *
	 * @param {String} workflowName
	 * @param {unknown} payload
	 * @param {object} opts
	 * @returns
	 */
	async run(workflowName, payload, opts) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Add job event to Redis.
	 *
	 * @param {String} workflowName The workflow object.
	 * @param {string} jobId The ID of the job.
	 * @param {Object} event The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	async addJobEvent(workflowName, jobId, event) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Save state of a job.
	 *
	 * @param {string} workflowName The name of workflow.
	 * @param {string} jobId The ID of the job.
	 * @param {Object} state The state object to save.
	 * @returns {Promise<void>} Resolves when the state is saved.
	 */
	async saveJobState(workflowName, jobId, state) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Get state of a workflow run.
	 *
	 * @param {string} workflowName
	 * @param {string} jobId
	 * @returns {Promise<unknown>} Resolves with the state object or null if not found.
	 */
	async getState(workflowName, jobId) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param {string} signalName The name of the signal to trigger.
	 * @param {string} key The key associated with the signal.
	 * @param {Object} payload The payload to send with the signal.
	 * @returns {Promise<void>} Resolves when the signal is triggered.
	 */
	async triggerSignal(signalName, key, payload) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Remove a named signal.
	 *
	 * @param {string} signalName The name of the signal to trigger.
	 * @param {string} key The key associated with the signal.
	 * @returns {Promise<void>} Resolves when the signal is triggered.
	 */
	async removeSignal(signalName, key) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Wait for a named signal.
	 *
	 * @param {string} signalName The name of the signal to wait for.
	 * @param {string} key The key associated with the signal.
	 * @param {Object} opts Options for waiting for the signal.
	 * @returns {Promise<*>} The payload of the received signal.
	 */
	async waitForSignal(signalName, key, opts) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Create a new job.
	 *
	 * @param {string} workflowName
	 * @param {*} payload
	 * @param {*} opts
	 * @returns {Promise<any>}
	 */
	async createJob(workflowName, payload, opts) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Get a job details.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @param {string[]|boolean} fields - The fields to retrieve or true to retrieve all fields.
	 * @returns {Promise<Object|null>} Resolves with the job object or null if not found.
	 */
	async getJob(workflowName, jobId, fields) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Get job events from Redis.
	 *
	 * @param {string} workflowName - The name of the workflow.
	 * @param {string} jobId - The ID of the job.
	 * @returns {Promise<Object[]>} Resolves with an array of job events.
	 */
	async getJobEvents(workflowName, jobId) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Get the next delayed jobs maintenance time.
	 */
	async getNextDelayedJobTime() {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * List all completed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<string[]>}
	 */
	async listCompletedJobs(workflowName) {
		throw new Error("Not implemented");
	}

	/**
	 * List all failed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<string[]>}
	 */
	async listFailedJobs(workflowName) {
		throw new Error("Not implemented");
	}

	/**
	 * List all delayed job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<string[]>}
	 */
	async listDelayedJobs(workflowName) {
		throw new Error("Not implemented");
	}

	/**
	 * List all active job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<string[]>}
	 */
	async listActiveJobs(workflowName) {
		throw new Error("Not implemented");
	}

	/**
	 * List all waiting job IDs for a workflow.
	 * @param {string} workflowName
	 * @returns {Promise<string[]>}
	 */
	async listWaitingJobs(workflowName) {
		throw new Error("Not implemented");
	}

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 *
	 * @param {string=} workflowName
	 * @param {string=} jobId
	 * @returns {Promise<void>}
	 */
	async cleanUp(workflowName, jobId) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Acquire a maintenance lock for a workflow.
	 *
	 * @param {number} lockTime - The time to hold the lock in milliseconds.
	 * @param {string} lockName - Lock name
	 * @returns {Promise<boolean>} Resolves with true if the lock is acquired, false otherwise.
	 */
	async lockMaintenance(lockTime, lockName = C.QUEUE_MAINTENANCE_LOCK) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Release the maintenance lock for a workflow.
	 *
	 * @param {string} lockName - Lock name
	 * @returns {Promise<void>} Resolves when the lock is released.
	 */
	async unlockMaintenance(lockName = C.QUEUE_MAINTENANCE_LOCK) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}
}

module.exports = BaseAdapter;
