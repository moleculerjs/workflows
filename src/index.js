/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { Context, METRIC } = require("moleculer");
const { BrokerOptionsError, ServiceSchemaError, MoleculerError } = require("moleculer").Errors;
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
		workflowHandlerTrigger: "emitLocalWorkflowHandler"
	});

	/** @type {ServiceBroker} */
	let broker;
	/** @type {Logger} */
	let logger;
	/** @type {BaseAdapter} */
	let adapter;
	/** @type {boolean} */
	let started = false;

	const wfList = [];

	/**
	 *
	 * @param {ServiceBroker} broker
	 */
	function registerWorkflowMetrics(broker) {
		if (!broker.isMetricsEnabled()) return;

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_CHANNELS_MESSAGES_SENT,
			labelNames: ["channel"],
			rate: true,
			unit: "call"
		});

		broker.metrics.register({
			type: METRIC.TYPE_COUNTER,
			name: C.METRIC_CHANNELS_MESSAGES_TOTAL,
			labelNames: ["channel", "group"],
			rate: true,
			unit: "msg"
		});

		broker.metrics.register({
			type: METRIC.TYPE_GAUGE,
			name: C.METRIC_CHANNELS_MESSAGES_ACTIVE,
			labelNames: ["channel", "group"],
			rate: true,
			unit: "msg"
		});

		broker.metrics.register({
			type: METRIC.TYPE_HISTOGRAM,
			name: C.METRIC_CHANNELS_MESSAGES_TIME,
			labelNames: ["channel", "group"],
			quantiles: true,
			unit: "msg"
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
			adapter.init(broker, logger);

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

				broker.metrics.increment(
					C.METRIC_WORKFLOWS_RUN_TOTAL,
					{ workflow: workflowName },
					1
				);
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
			 * Trigger a named signal.
			 *
			 * @param {string} signalName
			 * @param {unknown} key
			 * @param {unknown} payload
			 * @returns
			 */
			broker.wf.signal = (signalName, key, payload) => {
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
				broker.metrics.increment(
					C.METRIC_WORKFLOWS_SIGNAL_TOTAL,
					{ signal: signalName },
					1
				);
				return adapter.triggerSignal(signalName, key, payload);
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

			registerWorkflowMetrics(broker);
		},

		/**
		 * Created lifecycle hook of service
		 *
		 * @param {Service} svc
		 */
		async serviceCreated(svc) {
			if (_.isPlainObject(svc.schema[mwOpts.schemaProperty])) {
				// Process `workflows` in the schema
				await broker.Promise.mapSeries(
					Object.entries(svc.schema[mwOpts.schemaProperty]),
					async ([name, def]) => {
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

						if (!wf.name) wf.name = svc.fullName + "." + name;

						adapter.checkWorkflowName(wf.name);

						// Wrap the original handler
						let handler = broker.Promise.method(wf.handler).bind(svc);

						// Wrap the handler with custom middlewares
						const handler2 = broker.middlewares.wrapHandler(
							"localWorkflow",
							handler,
							wf
						);

						wf.handler = handler2;

						// Add metrics for the handler
						if (broker.isMetricsEnabled()) {
							wf.handler = (...args) => {
								const labels = { workflow: wf.name };
								const timeEnd = broker.metrics.timer(
									C.METRIC_WORKFLOWS_EXECUTIONS_TIME,
									labels
								);
								broker.metrics.increment(
									C.METRIC_WORKFLOWS_EXECUTIONS_TOTAL,
									labels
								);
								broker.metrics.increment(
									C.METRIC_WORKFLOWS_EXECUTIONS_ACTIVE,
									labels
								);
								return handler2(...args)
									.then(res => {
										timeEnd();
										broker.metrics.decrement(
											C.METRIC_WORKFLOWS_EXECUTIONS_ACTIVE,
											labels
										);
										return res;
									})
									.catch(err => {
										timeEnd();
										broker.metrics.decrement(
											C.METRIC_WORKFLOWS_EXECUTIONS_ACTIVE,
											labels
										);

										throw err;
									});
							};
						}

						wf.service = svc;

						// Register thw workflow handler into the adapter
						adapter.registerWorkflow(wf);
						wfList.push(wf);
						logger.info(`Workflow '${wf.name}' is registered.`);
					}
				);

				/**
				 * Call a local channel event handler. Useful for unit tests.
				 *
				 * @param {String} workflowName
				 * @param {Object} payload
				 * @param {string?} workflowId
				 * @returns
				 */
				svc[mwOpts.channelHandlerTrigger] = (workflowName, payload, workflowId) => {
					if (!workflowId) {
						workflowId = broker.generateUid();
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

					/** @type {Context} */
					const ctx = Context.create(broker, null, payload, { workflowId });

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
				};
			}
		},

		/**
		 * Service stopping lifecycle hook.
		 * Need to unregister workflows.
		 *
		 * @param {Service} svc
		 */
		async serviceStopping(svc) {
			wfList.forEach(wf => {
				adapter.unregisterWorkflow(wf);
				logger.info(`Workflow '${wf.name}' is unregistered.`);
			});
		},

		/**
		 * Start lifecycle hook of ServiceBroker
		 */
		async started() {
			logger.info("Workflows adapter is connecting...");
			await adapter.connect();
			logger.debug("Workflows adapter connected.");

			started = true;
		},

		/**
		 * Stop lifecycle hook of ServiceBroker
		 */
		async stopped() {
			logger.info("Workflows adapter is disconnecting...");
			await adapter.disconnect();
			logger.debug("Workflows adapter disconnected.");

			started = false;
		}
	};
};
