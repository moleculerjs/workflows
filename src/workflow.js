/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const {
	WorkflowError,
	WorkflowTaskMismatchError,
	WorkflowSignalTimeoutError
} = require("./errors");
const C = require("./constants");
const { parseDuration, getCronNextTime } = require("./utils");
const Adapters = require("./adapters");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").Service} Service Moleculer Service definition
 * @typedef {import("moleculer").Context} Context Moleculer Context
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("moleculer").Serializer} Serializer Moleculer Serializer

 * @typedef {import("./index.d.ts").BaseAdapter} BaseAdapter
 * @typedef {import("./index.d.ts").WorkflowSchema} WorkflowSchema
 * @typedef {import("./index.d.ts").WorkflowOptions} WorkflowOptions
 * @typedef {import("./index.d.ts").WorkflowHandler} WorkflowHandler Workflow handler
 * @typedef {import("./index.d.ts").WorkflowsMiddlewareOptions} WorkflowsMiddlewareOptions Middleware options
 * @typedef {import("./index.d.ts").WorkflowContext} WorkflowContext
 * @typedef {import("./index.d.ts").Job} Job
 * @typedef {import("./index.d.ts").JobEvent} JobEvent
 * @typedef {import("./index.d.ts").CreateJobOptions} CreateJobOptions
 */

/**
 * Workflow class
 *
 * @class WorkflowClass
 * @typedef {import("./index.d.ts").Workflow} WorkflowClass
 */
class Workflow {
	/**
	 * Constructor of workflow
	 * @param  {WorkflowSchema} schema
	 * @param  {Service?} svc
	 */
	constructor(schema, svc) {
		/** @type {WorkflowOptions} */
		this.opts = _.defaultsDeep({}, schema, {
			concurrency: 1
		});

		/** @type {string} */
		this.name = this.opts.name;

		/** @type {Service} */
		this.svc = svc;

		/** @type {WorkflowHandler} */
		this.handler = schema.handler;

		/** @type {BaseAdapter} */
		this.adapter = null;

		/** @type {string[]} */
		this.activeJobs = [];

		/** @type {NodeJS.Timeout} Maintenance timer */
		this.maintenanceTimer = null;

		/** @type {boolean} Flag indicating the adapter's connection status */
		// this.connected = false;

		/** @type {number} Last retention time */
		this.lastRetentionTime = null;

		/** @type {number} Last delayed maintenance time */
		this.delayedNextTime = null;

		/** @type {NodeJS.Timeout} Delayed maintenance timer */
		this.delayedTimer = null;
	}

	/**
	 * Initialize the workflow.
	 *
	 * @param {ServiceBroker} broker
	 * @param {Logger} logger
	 * @param {WorkflowsMiddlewareOptions} mwOpts - Middleware options.
	 */
	init(broker, logger, mwOpts) {
		/** @type {ServiceBroker} */
		this.broker = broker;
		/** @type {Logger} */
		this.logger = logger;
		/** @type {WorkflowsMiddlewareOptions} */
		this.mwOpts = mwOpts;
	}

	/**
	 * Log a message with the given level.
	 *
	 * @param {string} level
	 * @param {string|null} jobId
	 * @param {string} msg
	 * @param  {...any} args
	 */
	log(level, jobId, msg, ...args) {
		if (this.logger) {
			const wfJobName = jobId ? `${this.name}:${jobId}` : this.name;
			this.logger[level](`[${wfJobName}] ${msg}`, ...args);
		}
	}

	/**
	 * Start the workflow.
	 */
	async start() {
		this.adapter = Adapters.resolve(this.mwOpts.adapter);
		await this.adapter.init(this, this.broker, this.logger, this.mwOpts);

		await this.adapter.connect();
		// this.connected = true;
		await this.afterAdapterConnected();

		this.log("info", null, `Workflow '${this.name}' is started.`);
	}

	async afterAdapterConnected() {
		this.adapter.startJobProcessor();
		this.setNextDelayedMaintenance();

		if (!this.maintenanceTimer) {
			this.setNextMaintenance();
		}
	}

	/**
	 * Stop the workflow.
	 */
	async stop() {
		if (this.activeJobs.length > 0) {
			this.logger.warn(
				`Disconnecting adapter while there are ${this.activeJobs.length} active workflow jobs. This may cause data loss.`
			);
		}

		if (this.maintenanceTimer) {
			clearTimeout(this.maintenanceTimer);
			this.maintenanceTimer = null;
		}

		await this.adapter?.stopJobProcessor();

		// Close adapter
		await this.adapter?.disconnect();

		this.log("info", null, `Workflow '${this.name}' is stopped.`);
	}

	addRunningJob(jobId) {
		if (!this.activeJobs.includes(jobId)) {
			this.activeJobs.push(jobId);
		}
	}
	removeRunningJob(jobId) {
		const index = this.activeJobs.indexOf(jobId);
		if (index > -1) {
			this.activeJobs.splice(index, 1);
		}
	}

	getNumberOfActiveJobs() {
		return this.activeJobs.length;
	}

	/**
	 * Increment a metric.
	 *
	 * @param {string} metricName
	 */
	metricsIncrement(metricName) {
		if (!this.broker.isMetricsEnabled()) return;

		this.broker.metrics.increment(metricName, { workflow: this.name });
	}

	/**
	 * Create a workflow context for the given workflow and job.
	 *
	 * @param {Job} job The job object.
	 * @param {JobEvent[]} events The list of events associated with the job.
	 * @returns {WorkflowContext} The created workflow context.
	 */
	createWorkflowContext(job, events) {
		let taskId = 0;

		const ctxOpts = {};
		/** @type {WorkflowContext} */
		// @ts-ignore
		const ctx = this.broker.ContextFactory.create(this.broker, null, job.payload, ctxOpts);
		// @ts-ignore
		ctx.wf = {
			name: this.name,
			jobId: job.id,
			retryAttempts: job.retryAttempts,
			retries: job.retries,
			timeout: job.timeout ?? this.opts.timeout
		};

		const maxEventTaskId = Math.max(
			0,
			...(events || []).filter(e => e.type == "task").map(e => e.taskId || 0)
		);

		const getCurrentTaskEvent = () => {
			const event = events?.findLast(e => e.type == "task" && e.taskId === taskId);
			if (event?.error) {
				if (taskId == maxEventTaskId) {
					// If it's the last task, we don't throw the error because it should retry to execute it.
					return null;
				}
			}
			return event;
		};

		const taskEvent = async (taskType, data, startTime) => {
			return await this.adapter.addJobEvent(this.name, job.id, {
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
					job.id,
					"Workflow task already executed, skipping.",
					taskId,
					event
				);
				if (event.error) {
					const err = this.broker.errorRegenerator.restore(event.error, {});
					// err.stack = event.error.stack;
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

			// Sleep-start event
			const event = getCurrentTaskEvent();
			if (event) {
				validateEvent(event, "sleep-start");
			} else {
				await taskEvent("sleep-start", { time }, startTime);
			}

			// Sleep-end event
			taskId++;
			const event2 = getCurrentTaskEvent();
			if (event2) return validateEvent(event2, "sleep-end");

			let span;
			if (ctx.tracing) {
				span = ctx.startSpan(`sleep '${time}'`, {
					tags: {
						workflow: this.name,
						jobId: job.id,
						sleep: time
					}
				});
			}

			let remaining = parseDuration(time) - (event ? startTime - event.ts : 0);
			if (remaining > 0) {
				await new Promise(resolve => setTimeout(resolve, remaining));
			}

			await taskEvent("sleep-end", { time }, startTime);

			if (span) {
				ctx.finishSpan(span);
			}
		};

		ctx.wf.setState = async state => {
			taskId++;

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "state");

			await this.adapter.saveJobState(this.name, job.id, state);

			await taskEvent("state", { state });
		};

		ctx.wf.waitForSignal = async (signalName, key, opts) => {
			taskId++;
			const startTime = Date.now();

			// Signal-wait event
			const event = getCurrentTaskEvent();
			if (event) {
				validateEvent(event, "signal-wait");
			} else {
				await taskEvent(
					"signal-wait",
					{ signalName, signalKey: key, timeout: opts?.timeout },
					startTime
				);
			}

			// Signal-end event
			taskId++;
			const event2 = getCurrentTaskEvent();
			if (event2) return validateEvent(event2, "signal-end");

			let span;
			try {
				if (ctx.tracing) {
					span = ctx.startSpan(`signal wait '${signalName}'`, {
						tags: {
							workflow: this.name,
							jobId: job.id,
							signal: signalName,
							signalKey: key
						}
					});
				}
				const result = await Promise.race([
					this.adapter.waitForSignal(signalName, key, opts),
					new Promise((_, reject) => {
						if (opts?.timeout) {
							let remaining =
								parseDuration(opts.timeout) -
								(Date.now() - (event?.ts || startTime));
							if (remaining <= 0) {
								// Give a chance to waitForSignal to check the signal
								remaining = 1000;
							}
							setTimeout(() => {
								reject(
									new WorkflowSignalTimeoutError(signalName, key, opts.timeout)
								);
							}, remaining);
						}
					})
				]);

				await taskEvent(
					"signal-end",
					{ result, signalName, signalKey: key, timeout: opts?.timeout },
					startTime
				);

				if (span) {
					ctx.finishSpan(span);
				}

				return result;
			} catch (err) {
				await taskEvent(
					"signal-end",
					{
						error: err ? this.broker.errorRegenerator.extractPlainError(err) : true,
						signalName,
						signalKey: key,
						timeout: opts?.timeout
					},
					startTime
				);

				if (span) {
					span.setError(err);
					ctx.finishSpan(span);
				}

				throw err;
			}
		};

		ctx.wf.task = async (taskName, fn) => {
			taskId++;
			const startTime = Date.now();

			let span;
			if (ctx.tracing) {
				span = ctx.startSpan(`task '${taskName}'`, {
					tags: {
						workflow: this.name,
						jobId: job.id
					}
				});
			}

			if (!taskName) taskName = `custom-${taskId}`;
			if (!fn) throw new WorkflowError("Missing function to run.", 400, "MISSING_FUNCTION");

			const event = getCurrentTaskEvent();
			if (event) return validateEvent(event, "custom");

			try {
				const result = await fn();
				await taskEvent("custom", { taskName, result }, startTime);
				if (span) {
					ctx.finishSpan(span);
				}
				return result;
			} catch (err) {
				await taskEvent(
					"custom",
					{
						taskName,
						error: err ? this.broker.errorRegenerator.extractPlainError(err) : true
					},
					startTime
				);
				if (span) {
					span.setError(err);
					ctx.finishSpan(span);
				}
				throw err;
			}
		};

		return ctx;
	}

	/**
	 * Call workflow handler with a job.
	 *
	 * @param {Job} job The job object.
	 * @param {Array<Object>} events The list of events associated with the job.
	 * @returns {Promise<unknown>} The result of the workflow handler execution.
	 */
	async callHandler(job, events) {
		/** @type {WorkflowContext} */
		const ctx = this.createWorkflowContext(job, events);

		const result = await this.handler(ctx);
		return result;
	}

	getRoundedNextTime(time) {
		// Rounding next time + a small random time
		return Math.floor(Date.now() / time) * time + time + Math.floor(Math.random() * 100);
	}

	/**
	 * Calculate the next maintenance time. We use 'circa' to randomize the time a bit
	 * and avoid that all adapters run the maintenance at the same time.
	 */
	setNextMaintenance() {
		if (this.maintenanceTimer) {
			clearTimeout(this.maintenanceTimer);
		}

		let nextTime = this.getRoundedNextTime(this.mwOpts.maintenanceTime * 1000);
		// console.log("Set next maintenance time:", new Date(nextTime).toISOString());

		// If next time is too close, set it to 1 second later
		if (nextTime < Date.now() + 1000) {
			nextTime = Date.now() + 1000;
		}

		this.maintenanceTimer = setTimeout(() => this.maintenance(), nextTime - Date.now());
	}

	/**
	 * Run the maintenance tasks.
	 */
	async maintenance() {
		if (!this.adapter?.connected) return;

		if (await this.adapter.lockMaintenance(this.mwOpts.maintenanceTime * 1000)) {
			await this.adapter.maintenanceStalledJobs();
			await this.adapter.maintenanceActiveJobs();

			if (this.opts.retention) {
				const retention = parseDuration(this.opts.retention);
				if (retention > 0) {
					// Execution time is 1 minute, or the retention time if it's less.
					const executionTime = Math.min(retention, 60 * 1000);

					if (
						!this.lastRetentionTime ||
						this.lastRetentionTime + executionTime < Date.now()
					) {
						await Promise.all([
							await this.adapter.maintenanceRemoveOldJobs(
								C.QUEUE_COMPLETED,
								retention
							),
							await this.adapter.maintenanceRemoveOldJobs(C.QUEUE_FAILED, retention)
						]);

						this.lastRetentionTime = Date.now();
					}
				}
			}
		}

		this.setNextMaintenance();
	}

	/**
	 * Run the delayed jobs maintenance tasks.
	 */
	async maintenanceDelayed() {
		if (
			await this.adapter.lockMaintenance(
				this.mwOpts.maintenanceTime * 1000,
				C.QUEUE_MAINTENANCE_LOCK_DELAYED
			)
		) {
			await this.adapter.maintenanceDelayedJobs();
			await this.adapter.unlockMaintenance(C.QUEUE_MAINTENANCE_LOCK_DELAYED);
		}
		await this.setNextDelayedMaintenance();
	}

	/**
	 * Set the next delayed jobs maintenance timer for a workflow.
	 * @param {number} [nextTime] - Optional timestamp to schedule next maintenance.
	 */
	async setNextDelayedMaintenance(nextTime) {
		if (nextTime == null) {
			nextTime = await this.adapter.getNextDelayedJobTime();
		}

		const now = Date.now();
		if (!this.delayedNextTime || nextTime == null || nextTime < this.delayedNextTime) {
			clearTimeout(this.delayedTimer);

			let delay;
			if (nextTime != null) {
				delay = Math.max(0, nextTime - now + Math.floor(Math.random() * 50));
				this.log(
					"debug",
					null,
					"Set next delayed maintenance time:",
					new Date(nextTime).toISOString()
				);
			} else {
				const nextTime = this.getRoundedNextTime(this.mwOpts.maintenanceTime * 1000);
				delay = nextTime - now;
			}

			this.delayedTimer = setTimeout(async () => this.maintenanceDelayed(), delay);
		}
	}

	/**
	 * Check if the workflow name is valid.
	 *
	 * @param {String} workflowName
	 */
	static checkWorkflowName(workflowName) {
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
	 * @param {String} signalName
	 * @param {string|null} key
	 */
	static checkSignal(signalName, key) {
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

		if (key != null && !re.test(key)) {
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

	/**
	 * Create a new job and push it to the waiting or delayed queue.
	 *
	 * @param {BaseAdapter} adapter - The adapter instance.
	 * @param {string} workflowName - The name of the workflow.
	 * @param {unknown} payload - The job payload.
	 * @param {CreateJobOptions} opts - Additional options for the job.
	 * @returns {Promise<Job>} Resolves with the created job object.
	 */
	static async createJob(adapter, workflowName, payload, opts) {
		opts = opts || {};

		let isCustomJobId = !!opts.jobId;

		/** @type {Job} */
		let job = {
			id: opts.jobId ? adapter.checkJobId(opts.jobId) : adapter.broker.generateUid(),
			createdAt: Date.now()
		};

		if (payload != null) {
			job.payload = payload;
		}

		if (opts.retries != null) {
			job.retries = opts.retries;
			job.retryAttempts = 0;
		}

		if (opts.timeout) {
			job.timeout = parseDuration(opts.timeout);
		}

		if (opts.delay) {
			job.delay = parseDuration(opts.delay);
			job.promoteAt = Date.now() + job.delay;
		}

		if (opts.repeat) {
			if (!opts.jobId) {
				throw new WorkflowError(
					"Job ID is required for repeatable jobs",
					400,
					"MISSING_REPEAT_JOB_ID"
				);
			}

			job.repeat = opts.repeat;
			job.repeatCounter = 0;

			if (opts.repeat.endDate) {
				const endDate = new Date(opts.repeat.endDate).getTime();
				if (endDate < Date.now()) {
					throw new WorkflowError(
						"Repeatable job is expired at " +
							new Date(opts.repeat.endDate).toISOString(),
						400,
						"REPEAT_JOB_EXPIRED",
						{
							jobId: job.id,
							endDate: opts.repeat.endDate
						}
					);
				}
			}
		}

		// Save the Job to Redis
		return await adapter.newJob(workflowName, job, { isCustomJobId });
	}

	/**
	 * Reschedule a repeatable job based on its configuration.
	 *
	 * @param {BaseAdapter} adapter - The adapter instance.
	 * @param {string} workflowName - The name of workflow.
	 * @param {Job|string} job - The job object or job ID to reschedule.
	 * @returns {Promise<void>} Resolves when the job is rescheduled.
	 */
	static async rescheduleJob(adapter, workflowName, job) {
		try {
			if (typeof job == "string") {
				const jobId = job;
				job = await adapter.getJob(workflowName, job, [
					"payload",
					"repeat",
					"repeatCounter",
					"retries",
					"startedAt",
					"timeout"
				]);

				if (!job) {
					adapter.log(
						"warn",
						workflowName,
						jobId,
						"Parent job not found. Not rescheduling."
					);
					return;
				}
			}

			const nextJob = { ...job };
			delete nextJob.repeat;
			nextJob.createdAt = Date.now();
			nextJob.parent = job.id;

			if (job.repeat.cron) {
				if (job.repeat.endDate) {
					const endDate = new Date(job.repeat.endDate).getTime();
					if (endDate < Date.now()) {
						adapter.log(
							"debug",
							workflowName,
							job.id,
							`Repeatable job is expired at ${job.repeat.endDate}. Not rescheduling.`,
							job
						);

						await adapter.finishParentJob(workflowName, job.id);
						return;
					}
				}
				if (job.repeat.limit > 0) {
					if (job.repeatCounter >= job.repeat.limit) {
						adapter.log(
							"debug",
							workflowName,
							job.id,
							`Repeatable job reached the limit of ${job.repeat.limit}. Not rescheduling.`,
							job
						);

						await adapter.finishParentJob(workflowName, job.id);
						return;
					}
				}

				const promoteAt = getCronNextTime(job.repeat.cron, Date.now(), job.repeat.tz);
				if (!nextJob.promoteAt || nextJob.promoteAt < promoteAt) {
					nextJob.promoteAt = promoteAt;
				}

				nextJob.id = job.id + ":" + nextJob.promoteAt;

				await adapter.newRepeatChildJob(workflowName, nextJob);

				adapter.log(
					"debug",
					workflowName,
					job.id,
					`Scheduled job created. Next run: ${new Date(nextJob.promoteAt).toISOString()}`,
					nextJob
				);
			}
		} catch (err) {
			adapter.log(
				"error",
				workflowName,
				typeof job == "string" ? job : job.id,
				"Error while rescheduling job",
				err
			);
		}
	}
}

module.exports = Workflow;
