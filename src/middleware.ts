/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

import _ from "lodash";
import {
	METRIC,
	Errors,
	ServiceBroker,
	Logger,
	Service,
	Context,
	Middleware,
	ActionSchema
} from "moleculer";
import Workflow from "./workflow.ts";
import BaseAdapter, {
	ListJobResult,
	ListDelayedJobResult,
	ListFinishedJobResult
} from "./adapters/base.ts";
import Adapters from "./adapters/index.ts";

import * as C from "./constants.ts";
import Tracing from "./tracing.ts";
import type {
	WorkflowsMiddlewareOptions,
	WorkflowServiceBrokerMethods,
	Job,
	CreateJobOptions,
	JobEvent,
	WorkflowHandler
} from "./types.ts";
import type { WorkflowSchema } from "./workflow.ts";

/**
 * WorkflowsMiddleware for Moleculer
 */
export default function WorkflowsMiddleware(mwOpts: WorkflowsMiddlewareOptions): Middleware {
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

	let broker: ServiceBroker;
	let logger: Logger;
	let adapter: BaseAdapter | null;

	/**
	 * Register metrics for workflows
	 */
	function registerMetrics(broker: ServiceBroker) {
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
		 */
		created(_broker: ServiceBroker) {
			broker = _broker;
			logger = broker.getLogger("Workflows");

			// Populate broker with new methods
			if (!broker.wf) {
				broker.wf = {} as WorkflowServiceBrokerMethods;
			}

			broker.wf.getAdapter = async (): Promise<BaseAdapter> => {
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
			 * @param workflowName
			 * @param payload
			 * @param opts
			 */
			broker.wf.run = async (
				workflowName: string,
				payload: unknown,
				opts: CreateJobOptions
			): Promise<Job> => {
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
			 * @param workflowName
			 * @param jobId
			 * @returns
			 */
			broker.wf.remove = async (workflowName?: string, jobId?: string): Promise<void> => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new Errors.MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).cleanUp(workflowName, jobId);
			};

			/**
			 * Trigger a named signal.
			 *
			 * @param signalName
			 * @param key
			 * @param payload
			 * @returns
			 */
			broker.wf.triggerSignal = async (
				signalName: string,
				key?: string,
				payload?: unknown
			): Promise<void> => {
				if (!signalName) {
					return Promise.reject(
						new Errors.MoleculerError(
							"Signal name is required!",
							400,
							"SIGNAL_NAME_REQUIRED"
						)
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
			 * @param signalName
			 * @param key
			 * @returns
			 */
			broker.wf.removeSignal = async (signalName: string, key?: string): Promise<void> => {
				if (!signalName) {
					return Promise.reject(
						new Errors.MoleculerError(
							"Signal name is required!",
							400,
							"SIGNAL_NAME_REQUIRED"
						)
					);
				}

				Workflow.checkSignal(signalName, key);

				return (await broker.wf.getAdapter()).removeSignal(signalName, key);
			};

			/**
			 * Get state of a workflow run.
			 *
			 * @param workflowName
			 * @param jobId
			 * @returns
			 */
			broker.wf.getState = async (workflowName: string, jobId: string): Promise<unknown> => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new Errors.MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getState(workflowName, jobId);
			};

			/**
			 * Get job details of a workflow run.
			 *
			 * @param workflowName
			 * @param jobId
			 * @returns
			 */
			broker.wf.get = async (workflowName: string, jobId: string): Promise<Job> => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new Errors.MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getJob(workflowName, jobId, true);
			};

			/**
			 * Get job events of a workflow run.
			 *
			 * @param workflowName
			 * @param jobId
			 * @returns
			 */
			broker.wf.getEvents = async (
				workflowName: string,
				jobId: string
			): Promise<JobEvent[]> => {
				Workflow.checkWorkflowName(workflowName);

				if (!jobId) {
					return Promise.reject(
						new Errors.MoleculerError("Job ID is required!", 400, "JOB_ID_REQUIRED")
					);
				}
				return (await broker.wf.getAdapter()).getJobEvents(workflowName, jobId);
			};

			/**
			 * List completed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listCompletedJobs = async (
				workflowName: string
			): Promise<ListFinishedJobResult[]> => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listCompletedJobs(workflowName);
			};

			/**
			 * List failed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listFailedJobs = async (
				workflowName: string
			): Promise<ListFinishedJobResult[]> => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listFailedJobs(workflowName);
			};

			/**
			 * List delayed jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listDelayedJobs = async (
				workflowName: string
			): Promise<ListDelayedJobResult[]> => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listDelayedJobs(workflowName);
			};

			/**
			 * List active jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listActiveJobs = async (workflowName: string): Promise<ListJobResult[]> => {
				Workflow.checkWorkflowName(workflowName);

				return (await broker.wf.getAdapter()).listActiveJobs(workflowName);
			};

			/**
			 * List waiting jobs for a workflow.
			 *
			 * @param {string} workflowName
			 * @returns
			 */
			broker.wf.listWaitingJobs = async (workflowName: string): Promise<ListJobResult[]> => {
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
		 */
		async serviceCreated(svc: Service) {
			if (_.isPlainObject(svc.schema[mwOpts.schemaProperty])) {
				svc.$workflows = [];
				for (const [name, def] of Object.entries(svc.schema[mwOpts.schemaProperty])) {
					let wf: WorkflowSchema;
					if (_.isFunction(def)) {
						wf = { handler: def } as WorkflowSchema;
					} else if (_.isPlainObject(def)) {
						wf = _.cloneDeep(def) as WorkflowSchema;
					} else {
						throw new Errors.ServiceSchemaError(
							`Invalid workflow definition in '${name}' workflow in '${svc.fullName}' service!`,
							svc.schema
						);
					}
					if (wf.enabled === false) continue;

					if (!_.isFunction(wf.handler)) {
						throw new Errors.ServiceSchemaError(
							`Missing workflow handler on '${name}' workflow in '${svc.fullName}' service!`,
							svc.schema
						);
					}

					wf.name = wf.fullName ? wf.fullName : svc.fullName + "." + (wf.name || name);
					Workflow.checkWorkflowName(wf.name);

					// Wrap the original handler
					const handler = wf.handler.bind(svc);

					// Wrap the handler with custom middlewares
					const handler2 = broker.middlewares.wrapHandler(
						"localWorkflow",
						handler,
						wf as ActionSchema
					);

					wf.handler = handler2 as WorkflowHandler;

					// Add metrics for the handler
					if (broker.isMetricsEnabled()) {
						wf.handler = async (ctx: Context) => {
							const labels = { workflow: wf.name };
							const timeEnd = broker.metrics.timer(
								C.METRIC_WORKFLOWS_JOBS_TIME,
								labels
							);
							broker.metrics.increment(C.METRIC_WORKFLOWS_JOBS_ACTIVE, labels);
							try {
								const result = await handler2(ctx);
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
					if (wf.params && broker.validator) {
						const handler3 = wf.handler;

						const check = broker.validator.compile(wf.params);
						wf.handler = async (ctx: Context<Record<string, unknown>>) => {
							const res = await check(ctx.params != null ? ctx.params : {});
							if (res === true) return handler3(ctx);
							else {
								throw new Errors.ValidationError(
									"Parameters validation error!",
									null,
									res as unknown
								);
							}
						};
					}

					const workflow = new Workflow(wf, svc);
					await workflow.init(broker, logger, mwOpts);

					// Register the workflow handler into the adapter
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
				 *
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
				};
				*/
			}
		},

		/**
		 * Service started lifecycle hook.
		 * Need to register workflows.
		 */
		async serviceStarted(svc: Service) {
			if (!svc.$workflows) return;
			for (const wf of svc.$workflows) {
				await wf.start();
			}
		},

		/**
		 * Service stopping lifecycle hook.
		 * Need to unregister workflows.
		 */
		async serviceStopping(svc: Service) {
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
		},

		localWorkflow: null
	};

	if (mwOpts.tracing) {
		middleware.localWorkflow = Tracing;
	}

	return middleware;
}
