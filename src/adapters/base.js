/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { WorkflowError, WorkflowTaskMismatchError } = require("../errors");
const { Serializers } = require("moleculer");
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

			maintenanceTime: 10
		});

		/**
		 * Tracks the local running jobs per workflow.
		 * @type {Map<string, string>}
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
	init(broker, logger, mixinOpts) {
		this.broker = broker;
		this.logger = logger;
		this.mixinOpts = mixinOpts;

		// create an instance of serializer (default to JSON)
		/** @type {Serializer} */
		this.serializer = Serializers.resolve(this.opts.serializer);
		this.serializer.init(this.broker);
		this.logger.info("Workflows serializer:", this.broker.getConstructorName(this.serializer));

		this.setNextMaintenance();
	}

	/**
	 * Close the adapter.
	 */
	destroy() {
		const runningJobs = Array.from(this.activeRuns.values());
		if (runningJobs.size > 0) {
			this.logger.warn(
				`Disconnecting adapter while there are ${runningJobs.size} active workflow jobs. This may cause data loss.`
			);
		}

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
		if (this.connected) {
			this.startJobProcessor(workflow);
		}
	}

	/**
	 * Unregister a local workflow handler
	 *
	 * @param {Workflow} workflow
	 */
	async unregisterWorkflow(workflow) {
		this.workflows.delete(workflow.name);
		if (this.connected) {
			await this.stopJobProcessor(workflow);
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

		const maxEventTaskId = Math.max(
			0,
			...(events || []).filter(e => e.type == "task").map(e => e.taskId || 0)
		);

		const getCurrentTaskEvent = () => {
			const event = events?.find(e => e.type == "task" && e.taskId === taskId);
			if (event?.error) {
				if (taskId == maxEventTaskId) {
					// If it's the last task, we don't throw the error because it should retry to execute it.
					return null;
				}
			}
			return event;
		};

		const taskEvent = async (taskType, data, startTime) => {
			return await this.addJobEvent(workflow.name, job.id, {
				type: "task",
				taskId,
				taskType,
				duration: startTime ? Date.now() - startTime : undefined,
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
				throw new WorkflowTaskMismatchError(taskId, taskType, event.taskType);
			}
		};

		const wrapCtxMethod = (ctx, method, taskType, argProcessor) => {
			const originalMethod = ctx[method];
			ctx[method] = async (...args) => {
				const savedArgs = argProcessor ? argProcessor(args) : {};
				const startTime = Date.now();
				try {
					taskId++;

					const event = getCurrentTaskEvent();
					if (event) return validateEvent(event, taskType);

					const result = await originalMethod.apply(ctx, args);
					await taskEvent(taskType, { ...savedArgs, result }, startTime);
					return result;
				} catch (err) {
					await taskEvent(
						taskType,
						{
							...savedArgs,
							error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
						},
						startTime
					);
					throw err;
				}
			};
		};

		wrapCtxMethod(ctx, "call", "actionCall", args => ({ action: args[0] }));
		wrapCtxMethod(ctx, "mcall", "actionMcall");
		wrapCtxMethod(ctx, "broadcast", "actionBroadcast", args => ({ event: args[0] }));
		wrapCtxMethod(ctx, "emit", "eventEmit", args => ({ event: args[0] }));

		// Sleep method with Task event
		ctx.wf.sleep = async time => {
			taskId++;
			const startTime = Date.now();

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "sleep");

			await new Promise(resolve => setTimeout(resolve, time));

			await taskEvent("sleep", { time }, startTime);
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
			const startTime = Date.now();

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "signal");

			const result = await this.waitForSignal(signalName, key, opts);

			await taskEvent("signal", { result, signalName, signalKey: key }, startTime);

			return result;
		};

		ctx.wf.run = async (name, fn) => {
			taskId++;
			const startTime = Date.now();

			if (!name) name = `custom-${taskId}`;
			if (!fn) throw new WorkflowError("Missing function to run.", 400, "MISSING_FUNCTION");

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "custom");

			try {
				const result = await fn();
				await taskEvent("custom", { run: name, result }, startTime);
				return result;
			} catch (err) {
				await taskEvent(
					"custom",
					{
						run: name,
						error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
					},
					startTime
				);
				throw err;
			}
		};

		return ctx;
	}

	/**
	 * Add a running job to the list of active jobs.
	 *
	 * @param {Workflow} workflow
	 * @param {string} jobId
	 */
	addRunningJob(workflow, jobId) {
		const wf = this.activeRuns.get(workflow.name);
		if (!wf) {
			this.activeRuns.set(workflow.name, [jobId]);
		} else {
			wf.push(jobId);
		}
	}

	/**
	 * Remove a running job from the list of active jobs.
	 *
	 * @param {Workflow} workflow
	 * @param {string} jobId
	 */
	removeRunningJob(workflow, jobId) {
		const wf = this.activeRuns.get(workflow.name);
		if (wf) {
			const idx = wf.indexOf(jobId);
			if (idx > -1) {
				wf.splice(idx, 1);
			}
		}
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
		const ctx = this.createWorkflowContext(workflow, job, events);

		const result = await workflow.handler(ctx);
		return result;
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
	 * @param {String} workflowName The workflow object.
	 * @param {string} jobId The ID of the job.
	 * @param {Object} event The event object to add.
	 * @returns {Promise<void>} Resolves when the event is added.
	 */
	async addJobEvent(/*workflowName, jobId, event*/) {
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
	async getJobEvents(/*workflowName, jobId*/) {
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

						if (wf.retention) {
							const retention = parseDuration(wf.retention);
							if (retention > 0) {
								// Execution time is 1 minute, or the retention time if it's less.
								const executionTime = Math.min(retention, 60 * 1000);

								if (
									!wf.$lastRetentionTime ||
									wf.$lastRetentionTime + executionTime < Date.now()
								) {
									await this.maintenanceRemoveOldJobs(
										wf,
										C.QUEUE_COMPLETED,
										retention
									);
									await this.maintenanceRemoveOldJobs(
										wf,
										C.QUEUE_FAILED,
										retention
									);

									wf.$lastRetentionTime = Date.now();
								}
							}
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
	 * Send entity lifecycle events
	 *
	 * @param {String} workflowName
	 * @param {String} jobId
	 * @param {String} type
	 */
	sendJobEvent(workflowName, jobId, type) {
		if (this.mixinOpts?.jobEventType) {
			const eventName = `job.${workflowName}.${type}`;

			const payload = {
				type,
				workflow: workflowName,
				job: jobId
			};

			this.broker[this.mixinOpts.jobEventType](eventName, payload);
		}
	}

	/**
	 * Check if the workflow name is valid.
	 * @param {String} workflowName
	 */
	checkWorkflowName(workflowName) {
		const re = /^[a-zA-Z0-9_.-]+$/;
		if (!re.test(workflowName)) {
			throw new WorkflowError(
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
			throw new WorkflowError(
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
			throw new WorkflowError(
				`Invalid signal name '${signalName}'. Only alphanumeric characters, underscore, dot and dash are allowed.`,
				400,
				"INVALID_SIGNAL_NAME",
				{
					signalName
				}
			);
		}

		if (!re.test(key)) {
			throw new WorkflowError(
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
