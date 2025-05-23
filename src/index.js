/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { METRIC } = require("moleculer");
const { ServiceSchemaError, MoleculerError, ValidationError } = require("moleculer").Errors;
const Workflow = require("./workflow");
const Adapters = require("./adapters");
const C = require("./constants");
const Tracing = require("./tracing");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("moleculer").Service} Service Moleculer service
 * @typedef {import("moleculer").Middleware} Middleware Moleculer middleware
 *
 * @typedef {import("./index.d.ts").Workflow} Workflow Workflow definition
 * @typedef {import("./index.d.ts").WorkflowSchema} WorkflowSchema Workflow schema
 */

/** @param {WorkflowsMiddlewareOptions} mwOpts */
function WorkflowsMiddleware(mwOpts) {
	/** @type {WorkflowsMiddlewareOptions} */
	mwOpts = _.defaultsDeep({}, mwOpts, {
		adapter: "Redis",
		schemaProperty: "workflows",
		workflowHandlerTrigger: "emitLocalWorkflowHandler",
		jobEventType: null,
		signalExpiration: "1h",
		maintenanceTime: 10,
		lockExpiration: 30,
		jobIdCollision: "reject"
	});

	/** @type {ServiceBroker} */
	let broker;
	/** @type {Logger} */
	let logger;
	/** @type {BaseAdapter} */
	let adapter;

	/**
	 *
	 * @param {ServiceBroker} broker
	 */
	function registerMetrics(broker) {
		if (!broker.isMetricsEnabled()) return;

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_WORKFLOWS_JOBS_CREATED,
			labelNames: ["workflow"],
			rate: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_WORKFLOWS_JOBS_TOTAL,
			labelNames: ["workflow"],
			rate: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_GAUGE,
			name: C.METRIC_WORKFLOWS_JOBS_ACTIVE,
			labelNames: ["workflow"],
			rate: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_HISTOGRAM,
			name: C.METRIC_WORKFLOWS_JOBS_TIME,
			labelNames: ["workflow"],
			quantiles: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_GAUGE,
			name: C.METRIC_WORKFLOWS_JOBS_ERRORS_TOTAL,
			labelNames: ["workflow"],
			rate: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_GAUGE,
			name: C.METRIC_WORKFLOWS_JOBS_RETRIES_TOTAL,
			labelNames: ["workflow"],
			rate: true,
			unit: "job"
		});

		broker.metrics.register({
			type: METRIC.TYPE_GAUGE,
			name: C.METRIC_WORKFLOWS_SIGNALS_TOTAL,
			labelNames: ["signal"],
			rate: true,
			unit: "signal"
		});
	}

	const middleware = {
		name: "Workflows",

		/**
		 * Created lifecycle hook of ServiceBroker
		 *
		 * @param {ServiceBroker} _broker
		 */
		created(_broker) {
			broker = _broker;
			logger = broker.getLogger("Workflows");

			// Populate broker with new methods
			if (!broker.wf) {
				broker.wf = {};
			}

			broker.wf.getAdapter = async () => {
				if (!adapter) {
					adapter = Adapters.resolve(mwOpts.adapter);
					adapter.init(null, broker, logger, mwOpts);
					await adapter.connect();
				}
				return adapter;
			};

			/**
			 * Execute a workflow
			 *
			 * @param {String} workflowName
			 * @param {unknown} payload
			 * @param {CreateJobOptions} opts
			 * @returns {Promise<Job>}
			 */
			broker.wf.run = async (workflowName, payload, opts) => {
				Workflow.checkWorkflowName(workflowName);

				if (broker.isMetricsEnabled()) {
					broker.metrics.increment(C.METRIC_WORKFLOWS_JOBS_CREATED, {
						workflow: workflowName
					});
				}

				return Workflow.createJob(
					await broker.wf.getAdapter(),
					workflowName,
					payload,
					opts
				);
			};

			/**
			 * Remove a workflow job
			 *
			 * @param {String} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.remove = async (workflowName, jobId) => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).cleanUp(workflowName, jobId);
			};

			/**
			 * Trigger a named signal.
			 *
			 * @param {string} signalName
			 * @param {string} key
			 * @param {unknown} payload
			 * @returns
			 */
			broker.wf.triggerSignal = async (signalName, key, payload) => {
				if (!signalName) {
					return Promise.reject(
						new MoleculerError("Signal name is required!", 400, "SIGNAL_NAME_REQUIRED")
					);
				}

				Workflow.checkSignal(signalName, key);

				if (broker.isMetricsEnabled()) {
					broker.metrics.increment(C.METRIC_WORKFLOWS_SIGNALS_TOTAL, {
						signal: signalName
					});
				}
				return (await broker.wf.getAdapter()).triggerSignal(signalName, key, payload);
			};

			/**
			 * Remove a named signal.
			 *
			 * @param {string} signalName
			 * @param {string} key
			 * @returns
			 */
			broker.wf.removeSignal = async (signalName, key) => {
				if (!signalName) {
					return Promise.reject(
						new MoleculerError("Signal name is required!", 400, "SIGNAL_NAME_REQUIRED")
					);
				}

				Workflow.checkSignal(signalName, key);

				return (await broker.wf.getAdapter()).removeSignal(signalName, key);
			};

			/**
			 * Get state of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.getState = async (workflowName, jobId) => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getState(workflowName, jobId);
			};

			/**
			 * Get job details of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.get = async (workflowName, jobId) => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getJob(workflowName, jobId, true);
			};

			/**
			 * Get job events of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.getEvents = async (workflowName, jobId) => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getJobEvents(workflowName, jobId, true);
			};

			/**
			 * List completed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listCompletedJobs = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listCompletedJobs(workflowName);
			};

			/**
			 * List failed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listFailedJobs = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listFailedJobs(workflowName);
			};

			/**
			 * List delayed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listDelayedJobs = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listDelayedJobs(workflowName);
			};

			/**
			 * List active jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listActiveJobs = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listActiveJobs(workflowName);
			};

			/**
			 * List waiting jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listWaitingJobs = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listWaitingJobs(workflowName);
			};

			/**
			 * Delete all workflow jobs & history.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.cleanUp = async workflowName => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).cleanUp(workflowName);
			};

			registerMetrics(broker);
		},

		/**
		 * Created lifecycle hook of service
		 *
		 * @param {Service} svc
		 */
		async serviceCreated(svc) {
			if (_.isPlainObject(svc.schema[mwOpts.schemaProperty])) {
				svc.$workflows = [];

				// Process `workflows` in the schema
				for (const [name, def] of Object.entries(svc.schema[mwOpts.schemaProperty])) {
					/** @type {WorkflowSchema} */
					let wf;

					if (_.isFunction(def)) {
						wf = {
							handler: def
						};
					} else if (_.isPlainObject(def)) {
						wf = _.cloneDeep(def);
					} else {
						throw new ServiceSchemaError(
							`Invalid workflow definition in '${name}' workflow in '${svc.fullName}' service!`
						);
					}

					if (wf.enabled === false) {
						continue;
					}

					if (!_.isFunction(wf.handler)) {
						throw new ServiceSchemaError(
							`Missing workflow handler on '${name}' workflow in '${svc.fullName}' service!`
						);
					}

					wf.name = wf.fullName ? wf.fullName : svc.fullName + "." + (wf.name || name);
					Workflow.checkWorkflowName(wf.name);

					// Wrap the original handler
					let handler = broker.Promise.method(wf.handler).bind(svc);

					// Wrap the handler with custom middlewares
					const handler2 = broker.middlewares.wrapHandler("localWorkflow", handler, wf);

					wf.handler = handler2;

					// Add metrics for the handler
					if (broker.isMetricsEnabled()) {
						wf.handler = async (...args) => {
							const labels = { workflow: wf.name };
							const timeEnd = broker.metrics.timer(
								C.METRIC_WORKFLOWS_JOBS_TIME,
								labels
							);
							broker.metrics.increment(C.METRIC_WORKFLOWS_JOBS_ACTIVE, labels);
							try {
								const result = await handler2(...args);
								return result;
							} catch (err) {
								broker.metrics.increment(
									C.METRIC_WORKFLOWS_JOBS_ERRORS_TOTAL,
									labels
								);
								throw err;
							} finally {
								timeEnd();
								broker.metrics.decrement(C.METRIC_WORKFLOWS_JOBS_ACTIVE, labels);
								broker.metrics.increment(C.METRIC_WORKFLOWS_JOBS_TOTAL, labels);
							}
						};
					}

					if (wf.params) {
						const handler3 = wf.handler;

						const check = broker.validator.compile(wf.params);
						wf.handler = async ctx => {
							const res = await check(ctx.params != null ? ctx.params : {});
							if (res === true) return handler3(ctx);
							else {
								throw new ValidationError(
									"Parameters validation error!",
									null,
									res
								);
							}
						};
					}

					const workflow = new Workflow(wf, svc);
					await workflow.init(broker, logger, mwOpts);

					// Register thw workflow handler into the adapter
					svc.$workflows.push(workflow);
					logger.info(`Workflow '${workflow.name}' is registered.`);
				}

				/**
				 * Call a local channel event handler. Useful for unit tests.
				 * TODO:
				 *
				 * @param {String} workflowName
				 * @param {Object} payload
				 * @param {string?} jobId
				 * @returns
				 */
				svc[mwOpts.channelHandlerTrigger] = (workflowName, payload, jobId) => {
					if (!jobId) {
						jobId = broker.generateUid();
					}

					svc.logger.debug(
						`${mwOpts.channelHandlerTrigger} called '${workflowName}' workflow handler`
					);

					if (!svc.schema[mwOpts.schemaProperty][workflowName]) {
						return Promise.reject(
							new MoleculerError(
								`'${workflowName}' is not registered as local workflow event handler`,
								500,
								"NOT_FOUND_WORKFLOW",
								{ workflowName }
							)
						);
					}

					/* TODO:
					const ctx = (await broker.wf.getAdapter()).createWorkflowContext(workflow, job, events);

					// Shorthand definition
					if (typeof svc.schema[mwOpts.schemaProperty][workflowName] === "function")
						return svc.schema[mwOpts.schemaProperty][workflowName].call(
							svc, // Attach reference to service
							ctx
						);

					// Object definition
					return svc.schema[mwOpts.schemaProperty][workflowName].handler.call(
						svc, // Attach reference to service
						ctx
					);
					*/
				};
			}
		},

		/**
		 * Service started lifecycle hook.
		 * Need to register workflows.
		 * @param {*} svc
		 */
		async serviceStarted(svc) {
			if (!svc.$workflows) return;

			for (const wf of svc.$workflows) {
				await wf.start();
			}
		},

		/**
		 * Service stopping lifecycle hook.
		 * Need to unregister workflows.
		 *
		 * @param {Service} svc
		 */
		async serviceStopping(svc) {
			if (!svc.$workflows) return;

			for (const wf of svc.$workflows) {
				await wf.stop();
			}
		},

		/**
		 * Start lifecycle hook of ServiceBroker
		 */
		async started() {},

		/**
		 * Stop lifecycle hook of ServiceBroker
		 */
		async stopped() {
			if (adapter) {
				await adapter.disconnect();
				adapter = null;
			}
		}
	};

	if (mwOpts.tracing) {
		middleware.localWorkflow = Tracing;
	}

	return middleware;
}

module.exports = WorkflowsMiddleware;
