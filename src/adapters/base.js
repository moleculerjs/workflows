/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const semver = require("semver");
const { MoleculerError } = require("moleculer").Errors;
const { Serializers, METRIC } = require("moleculer");
const C = require("../constants");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").Service} Service Moleculer Service definition
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("moleculer").Serializer} Serializer Moleculer Serializer
 * @typedef {import("../index").Channel} Channel Base channel definition
 * @typedef {import("../index").DeadLetteringOptions} DeadLetteringOptions Dead-letter-queue options
 */

/**
 * @typedef {Object} BaseDefaultOptions Base Adapter configuration
 * @property {String?} prefix Adapter prefix
 * @property {String} serializer Type of serializer to use in message exchange. Defaults to JSON
 */

class BaseAdapter {
	/**
	 * Constructor of adapter
	 * @param  {Object?} opts
	 */
	constructor(opts) {
		/** @type {BaseDefaultOptions} */
		this.opts = _.defaultsDeep({}, opts, {
			serializer: "JSON"
		});

		/**
		 * Tracks the local running workflows
		 * @type {Array<string>}
		 */
		this.activeRuns = [];

		// Registered local workflow handlers
		this.workflows = new Map();

		/** @type {Boolean} Flag indicating the adapter's connection status */
		this.connected = false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {ServiceBroker} broker
	 * @param {Logger} logger
	 */
	init(broker, logger) {
		this.broker = broker;
		this.logger = logger;

		// create an instance of serializer (default to JSON)
		/** @type {Serializer} */
		this.serializer = Serializers.resolve(this.opts.serializer);
		this.serializer.init(this.broker);
		this.logger.info("Workflows serializer:", this.broker.getConstructorName(this.serializer));

		this.registerAdapterMetrics(broker);
	}

	/**
	 * Register adapter related metrics
	 * @param {ServiceBroker} broker
	 */
	registerAdapterMetrics(broker) {
		if (!broker.isMetricsEnabled()) return;

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_WORKFLOWS_EXECUTIONS_ERRORS_TOTAL,
			labelNames: ["workflow"],
			rate: true
		});

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_WORKFLOWS_EXECUTIONS_RETRIES_TOTAL,
			labelNames: ["workflow"],
			rate: true
		});
	}

	/**
	 *
	 * @param {String} metricName
	 * @param {Channel} chan
	 */
	metricsIncrement(metricName, chan) {
		if (!this.broker.isMetricsEnabled()) return;

		this.broker.metrics.increment(metricName, {
			channel: chan.name,
			group: chan.group
		});
	}

	/**
	 * Register a local workflow handler
	 *
	 * @param {Workflow} workflow
	 */
	registerWorkflow(workflow) {
		if (this.workflows.has(workflow.name)) {
			this.stopJobProcessor(workflow);
		}

		this.workflows.set(workflow.name, workflow);
		this.startJobProcessor(workflow);
	}

	/**
	 * Unregister a local workflow handler
	 *
	 * @param {Workflow} workflow
	 */
	unregisterWorkflow(workflow) {
		this.workflows.delete(workflow.name);
		this.stopJobProcessor(workflow);
	}

	/**
	 * Connect to the adapter.
	 */
	async connect() {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	async afterConnected() {
		this.workflows.forEach(workflow => {
			this.startJobProcessor(workflow);
		});
	}

	/**
	 * Disconnect from adapter
	 */
	async disconnect() {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	startJobProcessor(workflow) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	stopJobProcessor(workflow) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	async createJob(workflowName, payload, opts) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	async callWorkflowHandler(workflow, job, events) {
		await this.addJobEvent(workflow, job.id, {
			type: "started",
			ts: new Date()
		});

		await new Promise(resolve => setTimeout(resolve, 5 * 1000));

		await this.addJobEvent(workflow, job.id, {
			type: "finished",
			ts: new Date()
		});

		return { a: 5 };
	}

	/**
	 * Execute a workflow
	 *
	 * @param {String} workflowName
	 * @param {unknown} payload
	 * @param {object} opts
	 * @returns
	 */
	async run(/*workflowName, payload, opts*/) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	/**
	 * Add job event to Redis.
	 *
	 * @param {*} workflow
	 * @param {*} jobId
	 * @param {*} event
	 * @returns
	 */
	async addJobEvent(/*workflow, jobId, event*/) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	/**
	 * Trigger a named signal.
	 *
	 * @param {string} signalName
	 * @param {unknown} key
	 * @param {unknown} payload
	 * @returns
	 */
	async triggerSignal(/*signalName, key, payload*/) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	/**
	 * Get state of a workflow run.
	 *
	 * @param {string} workflowName
	 * @param {string} workflowId
	 * @returns
	 */
	async getState(/*workflowName, workflowId*/) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}

	/**
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 * If both are provided, the adapter should clean up only the job with the given ID.
	 * If only the workflow name is provided, the adapter should clean up all jobs
	 * related to that workflow.
	 * If neither is provided, the adapter should clean up all jobs.
	 *
	 * @param {string?} workflowName
	 * @param {string?} jobId
	 */
	async cleanUp(workflowName, jobId) {
		/* istanbul ignore next */
		throw new Error("This method is not implemented.");
	}
}

module.exports = BaseAdapter;
