/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { MoleculerError } = require("moleculer").Errors;
const { Serializers, METRIC } = require("moleculer");
const C = require("../constants");
const { circa, parseDuration } = require("../utils");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").Service} Service Moleculer Service definition
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("moleculer").Serializer} Serializer Moleculer Serializer
 * @typedef {import("../index").Workflow} Workflow Workflow definition
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
	 * @param  {BaseDefaultOptions?} opts
	 */
	constructor(opts) {
		/** @type {BaseDefaultOptions} */
		this.opts = _.defaultsDeep({}, opts, {
			serializer: "JSON",

			signalExpiration: "1h",

			maintenanceTime: 10,
			removeCompletedAfter: "30m",
			removeFailedAfter: "30m",

			backoff: "exponential",
			backoffDelay: 1000
		});

		/**
		 * Tracks the local running workflows
		 * @type {Array<string>}
		 */
		this.activeRuns = new Map();

		// Registered local workflow handlers
		this.workflows = new Map();

		/** @type {Boolean} Flag indicating the adapter's connection status */
		this.connected = false;

		this.maintenanceTimer = null;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {import("moleculer").ServiceBroker} broker
	 * @param {import("moleculer").LoggerInstance} logger
	 */
	init(broker, logger) {
		this.broker = broker;
		this.logger = logger;

		// create an instance of serializer (default to JSON)
		/** @type {Serializer} */
		this.serializer = Serializers.resolve(this.opts.serializer);
		this.serializer.init(this.broker);
		this.logger.info("Workflows serializer:", this.broker.getConstructorName(this.serializer));

		this.setNextMaintenance();
	}

	disconnect() {
		if (this.maintenanceTimer) {
			clearTimeout(this.maintenanceTimer);
		}
	}

	/**
	 * Log a message with the given level.
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
	 * Increment a metric.
	 * @param {String} metricName
	 * @param {String} workflow
	 */
	metricsIncrement(metricName, workflow) {
		if (!this.broker.isMetricsEnabled()) return;

		this.broker.metrics.increment(metricName, { workflow });
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
	 *
	 * @returns {Promise<void>}
	 */
	async connect() {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Called after the adapter is connected.
	 */
	async afterConnected() {
		this.workflows.forEach(workflow => {
			this.startJobProcessor(workflow);
		});
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
	 * Create a new job.
	 *
	 * @param {string} workflowName
	 * @param {*} payload
	 * @param {*} opts
	 * @returns {Promise<any>}
	 */
	async createJob(/*workflowName, payload, opts*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Create a workflow context for the given workflow and job.
	 *
	 * @param {Workflow} workflow The workflow object.
	 * @param {Job} job The job object.
	 * @param {Array<Object>} events The list of events associated with the job.
	 * @returns {Context} The created workflow context.
	 */
	createWorkflowContext(workflow, job, events) {
		let taskId = 0;

		const ctxOpts = {};
		const ctx = this.broker.ContextFactory.create(this.broker, null, job.payload, ctxOpts);
		ctx.wf = {
			name: workflow.name,
			jobId: job.id
		};

		const getCurrentTaskEvent = () =>
			events?.find(e => e.type == "task" && e.taskId === taskId);

		const taskEvent = async (taskType, data) => {
			return await this.addJobEvent(workflow, job.id, {
				type: "task",
				taskId,
				taskType,
				...(data ?? {})
			});
		};

		const validateEvent = (event, taskType) => {
			if (event.taskType == taskType) {
				this.log(
					"debug",
					workflow.name,
					job.id,
					"Workflow task already executed, skipping.",
					taskId,
					event
				);
				if (event.error) {
					const err = this.broker.errorRegenerator.restore(event.error);
					err.stack = event.error.stack;
					throw err;
				}

				return event.result;
			} else {
				throw new MoleculerError(
					`Workflow task mismatch at replaying. Expected '${taskType}' but got '${event.taskType}'.`,
					500,
					"WORKFLOW_TASK_MISMATCH",
					{
						taskId,
						expected: taskType,
						actual: event.taskType
					}
				);
			}
		};

		const wrapCtxMethod = (ctx, method, taskType, argProcessor) => {
			const originalMethod = ctx[method];
			ctx[method] = async (...args) => {
				const savedArgs = argProcessor ? argProcessor(args) : {};
				try {
					taskId++;

					const event = getCurrentTaskEvent();
					if (event) return validateEvent(event, taskType);

					const result = await originalMethod.apply(ctx, args);
					await taskEvent(taskType, { ...savedArgs, result });
					return result;
				} catch (err) {
					await taskEvent(taskType, {
						...savedArgs,
						error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
					});
					throw err;
				}
			};
		};

		wrapCtxMethod(ctx, "call", "actionCall", args => ({ action: args[0] }));
		wrapCtxMethod(ctx, "mcall", "actionMcall");
		wrapCtxMethod(ctx, "broadcast", "actionBroadcast", args => ({ event: args[0] }));
		wrapCtxMethod(ctx, "emit", "eventEmit", args => ({ event: args[0] }));

		// Sleep method with Task event
		ctx.wf.sleep = async duration => {
			taskId++;

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "sleep");

			await new Promise(resolve => setTimeout(resolve, duration));

			await taskEvent("sleep", { duration });
		};

		ctx.wf.setState = async state => {
			taskId++;

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "state");

			await this.saveJobState(workflow, job.id, state);

			await taskEvent("state", { state });
		};

		ctx.wf.waitForSignal = async (signalName, key, opts) => {
			taskId++;

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "signal");

			const result = await this.waitForSignal(signalName, key, opts);

			await taskEvent("signal", { result, signalName, signalKey: key });

			return result;
		};

		ctx.wf.run = async (name, fn) => {
			taskId++;

			if (!name) name = `custom-${taskId}`;
			if (!fn) throw new Error("Missing function to run.");

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "custom");

			try {
				const result = await fn();
				await taskEvent("custom", { run: name, result });
				return result;
			} catch (err) {
				await taskEvent("custom", {
					run: name,
					error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
				});
				throw err;
			}
		};

		return ctx;
	}

	/**
	 * Call workflow handler with a job.
	 *
	 * @param {Workflow} workflow The workflow object.
	 * @param {Job} job The job object.
	 * @param {Array<Object>} events The list of events associated with the job.
	 * @returns {Promise<*>} The result of the workflow handler execution.
	 */
	async callWorkflowHandler(workflow, job, events) {
		this.activeRuns.set(job.id, job);
		await this.addJobEvent(workflow, job.id, {
			type: "started"
		});

		try {
			const ctx = this.createWorkflowContext(workflow, job, events);

			const result = await workflow.handler(ctx);

			await this.addJobEvent(workflow, job.id, {
				type: "finished"
			});

			return result;
		} catch (err) {
			await this.addJobEvent(workflow, job.id, {
				type: "failed",
				error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
			});

			throw err;
		} finally {
			this.activeRuns.delete(job.id);
		}
	}

	/**
	 * Create a job
	 *
	 * @param {String} workflowName
	 * @param {unknown} payload
	 * @param {object} opts
	 * @returns
	 */
	async run(/*workflowName, payload, opts*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Add job event to Redis.
	 *
	 * @param {Workflow} workflow The workflow object.
	 * @param {string} jobId The ID of the job.
	 * @param {Object} event The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	async addJobEvent(/*workflow, jobId, event*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Save state of a job.
	 *
	 * @param {Workflow} workflow The workflow object.
	 * @param {string} jobId The ID of the job.
	 * @param {Object} state The state object to save.
	 * @returns {Promise<void>} Resolves when the state is saved.
	 */
	async saveJobState(/*workflow, jobId, state*/) {
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
	async triggerSignal(/*signalName, key, payload*/) {
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
	async removeSignal(/*signalName, key*/) {
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
	async waitForSignal(/*signalName, key, opts*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Get state of a workflow run.
	 *
	 * @param {string} workflowName
	 * @param {string} jobId
	 * @returns
	 */
	async getState(/*workflowName, jobId*/) {
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
	async getJob(/*workflowName, jobId, fields*/) {
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
	 * Clean up the adapter store. Workflowname and jobId are optional.
	 *
	 * @param {string?} workflowName
	 * @param {string?} jobId
	 * @returns {Promise<void>}
	 */
	async cleanUp(/*workflowName, jobId*/) {
		/* istanbul ignore next */
		throw new Error("Abstract method is not implemented.");
	}

	/**
	 * Calculate the next maintenance time. We use 'circa' to randomize the time a bit
	 * and avoid that all adapters run the maintenance at the same time.
	 */
	setNextMaintenance() {
		if (this.maintenanceTimer) {
			clearTimeout(this.maintenanceTimer);
		}

		this.maintenanceTimer = setTimeout(
			() => this.maintenance(),
			circa(this.opts.maintenanceTime * 1000)
		);
	}

	/**
	 *
	 */
	async maintenance() {
		await Promise.all(
			Array.from(this.workflows.values()).map(async wf => {
				if (await this.lockMaintenance(wf)) {
					try {
						await this.maintenanceDelayedJobs(wf);
						await this.maintenanceStalledJobs(wf);

						const completedDuration = parseDuration(this.opts.removeCompletedAfter);
						if (completedDuration > 0) {
							await this.maintenanceRemoveOldJobs(
								wf,
								C.QUEUE_COMPLETED,
								completedDuration
							);
						}

						const failedDuration = parseDuration(this.opts.removeFailedAfter);
						if (failedDuration > 0) {
							await this.maintenanceRemoveOldJobs(wf, C.QUEUE_FAILED, failedDuration);
						}
					} finally {
						await this.unlockMaintenance(wf);
					}
				}
			})
		);

		this.setNextMaintenance();
	}

	/**
	 * Check if the workflow name is valid.
	 * @param {String} workflowName
	 */
	checkWorkflowName(workflowName) {
		const re = /^[a-zA-Z0-9_.-]+$/;
		if (!re.test(workflowName)) {
			throw new MoleculerError(
				`Invalid workflow name '${workflowName}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_WORKFLOW_NAME",
				{
					workflowName
				}
			);
		}

		return workflowName;
	}

	/**
	 * Check if the job ID is valid.
	 *
	 * @param {String} jobId
	 */
	checkJobId(jobId) {
		const re = /^[a-zA-Z0-9_.-]+$/;
		if (!re.test(jobId)) {
			throw new MoleculerError(
				`Invalid job ID '${jobId}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_JOB_ID",
				{
					jobId
				}
			);
		}

		return jobId;
	}

	/**
	 * Check if the job ID is valid.
	 *
	 * @param {String} jobId
	 */
	checkSignal(signalName, key) {
		const re = /^[a-zA-Z0-9_.-]+$/;
		if (!re.test(signalName)) {
			throw new MoleculerError(
				`Invalid signal name '${signalName}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_SIGNAL_NAME",
				{
					signalName
				}
			);
		}

		if (!re.test(key)) {
			throw new MoleculerError(
				`Invalid signal key '${key}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_SIGNAL_KEY",
				{
					key
				}
			);
		}
	}
}

module.exports = BaseAdapter;
