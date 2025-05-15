/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { METRIC } = require("moleculer");
const { BrokerOptionsError, ServiceSchemaError, MoleculerError, ValidationError } =
	require("moleculer").Errors;
const Adapters = require("./adapters");
const C = require("./constants");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("moleculer").Service} Service Moleculer service
 * @typedef {import("moleculer").Middleware} Middleware Moleculer middleware
 * @typedef {import("./adapters/base")} BaseAdapter Base adapter class
 */

module.exports = function WorkflowsMiddleware(mwOpts) {
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
			name: C.METRIC_WORKFLOWS_SIGNAL_TOTAL,
			labelNames: ["signal"],
			rate: true,
			unit: "signal"
		});
	}

	return {
		name: "Workflows",

		/**
		 * Created lifecycle hook of ServiceBroker
		 *
		 * @param {ServiceBroker} _broker
		 */
		created(_broker) {
			broker = _broker;
			logger = broker.getLogger("Workflows");

			// Create store adapter
			if (!mwOpts.adapter)
				throw new BrokerOptionsError("Workflow adapter must be defined.", { opts: mwOpts });

			adapter = Adapters.resolve(mwOpts.adapter);
			adapter.init(broker, logger, mwOpts);

			// Populate broker with new methods
			if (!broker.wf) {
				broker.wf = {};
			}

			/**
			 * Execute a workflow
			 *
			 * @param {String} workflowName
			 * @param {unknown} payload
			 * @param {object} opts
			 * @returns
			 */
			broker.wf.run = (workflowName, payload, opts) => {
				adapter.checkWorkflowName(workflowName);

				if (broker.isMetricsEnabled()) {
					broker.metrics.increment(C.METRIC_WORKFLOWS_JOBS_CREATED, {
						workflow: workflowName
					});
				}
				return adapter.createJob(workflowName, payload, opts);
			};

			/**
			 * Remove a workflow job
			 *
			 * @param {String} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.remove = (workflowName, jobId) => {
				adapter.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return adapter.cleanUp(workflowName, jobId);
			};

			/**
			 * Cleanup workflow jobs and history
			 *
			 * @param {String} workflowName
			 * @returns
			 */
			broker.wf.cleanup = workflowName => {
				adapter.checkWorkflowName(workflowName);
				return adapter.cleanUp(workflowName);
			};

			/**
			 * Trigger a named signal.
			 *
			 * @param {string} signalName
			 * @param {unknown} key
			 * @param {unknown} payload
			 * @returns
			 */
			broker.wf.triggerSignal = (signalName, key, payload) => {
				if (!signalName) {
					return Promise.reject(
						new MoleculerError("Signal name is required!", 400, "SIGNAL_NAME_REQUIRED")
					);
				}

				if (!key) {
					return Promise.reject(
						new MoleculerError("Signal key is required!", 400, "SIGNAL_KEY_REQUIRED")
					);
				}

				adapter.checkSignal(signalName, key);

				if (broker.isMetricsEnabled()) {
					broker.metrics.increment(C.METRIC_WORKFLOWS_SIGNAL_TOTAL, {
						signal: signalName
					});
				}
				return adapter.triggerSignal(signalName, key, payload);
			};

			/**
			 * Remove a named signal.
			 *
			 * @param {string} signalName
			 * @param {unknown} key
			 * @returns
			 */
			broker.wf.removeSignal = (signalName, key) => {
				if (!signalName) {
					return Promise.reject(
						new MoleculerError("Signal name is required!", 400, "SIGNAL_NAME_REQUIRED")
					);
				}
				if (!key) {
					return Promise.reject(
						new MoleculerError("Signal key is required!", 400, "SIGNAL_KEY_REQUIRED")
					);
				}

				adapter.checkSignal(signalName, key);

				return adapter.removeSignal(signalName, key);
			};

			/**
			 * Get state of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.getState = (workflowName, jobId) => {
				adapter.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return adapter.getState(workflowName, jobId);
			};

			/**
			 * Get job details of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.get = (workflowName, jobId) => {
				adapter.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return adapter.getJob(workflowName, jobId, true);
			};

			/**
			 * Get job events of a workflow run.
			 *
			 * @param {string} workflowName
			 * @param {string} jobId
			 * @returns
			 */
			broker.wf.getEvents = (workflowName, jobId) => {
				adapter.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return adapter.getJobEvents(workflowName, jobId, true);
			};

			/**
			 * Delete all workflow jobs & history.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.cleanUp = workflowName => {
				adapter.checkWorkflowName(workflowName);

				return adapter.cleanUp(workflowName);
			};

			// Add adapter reference to the broker instance
			broker.wf.adapter = adapter;

			registerMetrics(broker);
		},

		/**
		 * Created lifecycle hook of service
		 *
		 * @param {Service} svc
		 */
		async serviceCreated(svc) {
			if (_.isPlainObject(svc.schema[mwOpts.schemaProperty])) {
				svc.$workflowList = [];

				// Process `workflows` in the schema
				for (const [name, def] of Object.entries(svc.schema[mwOpts.schemaProperty])) {
					/** @type {Partial<WorkFlow>} */
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

					if (!_.isFunction(wf.handler)) {
						throw new ServiceSchemaError(
							`Missing workflow handler on '${name}' workflow in '${svc.fullName}' service!`
						);
					}

					wf.name = wf.fullName ? wf.fullName : svc.fullName + "." + (wf.name || name);
					adapter.checkWorkflowName(wf.name);

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

					wf.service = svc;

					// Register thw workflow handler into the adapter
					svc.$workflowList.push(wf);
					logger.info(`Workflow '${wf.name}' is registered.`);
				}

				/**
				 * Call a local channel event handler. Useful for unit tests.
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
					const ctx = adapter.createWorkflowContext(workflow, job, events);

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
			if (!svc.$workflowList) return;

			for (const wf of svc.$workflowList) {
				// Register workflow into the adapter
				adapter.registerWorkflow(wf);
			}
		},

		/**
		 * Service stopping lifecycle hook.
		 * Need to unregister workflows.
		 *
		 * @param {Service} svc
		 */
		async serviceStopping(svc) {
			if (!svc.$workflowList) return;

			for (const wf of svc.$workflowList) {
				adapter.unregisterWorkflow(wf);
				logger.info(`Workflow '${wf.name}' is unregistered.`);
			}
		},

		/**
		 * Start lifecycle hook of ServiceBroker
		 */
		async started() {
			logger.info("Workflows adapter is connecting...");
			await adapter.connect();
			logger.debug("Workflows adapter connected.");
		},

		/**
		 * Stop lifecycle hook of ServiceBroker
		 */
		async stopped() {
			logger.info("Workflows adapter is disconnecting...");
			await adapter.destroy();
			logger.debug("Workflows adapter disconnected.");
		}
	};
};
