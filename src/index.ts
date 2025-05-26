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
	LoggerInstance as Logger,
	Service,
	Middleware,
	Context
} from "moleculer";
import Workflow from "./workflow";
import BaseAdapter from "./adapters/base";
import Adapters from "./adapters";

import * as C from "./constants";
import Tracing from "./tracing";
import type { WorkflowsMiddlewareOptions, Job, CreateJobOptions } from "./types";
import type { WorkflowSchema } from "./workflow";

/**
 * WorkflowsMiddleware for Moleculer
 */
export function WorkflowsMiddleware(mwOpts: WorkflowsMiddlewareOptions) {
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
				broker.wf = {};
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

			// ...existing code for all broker.wf methods, keeping JSDoc and comments...
			// ...existing code...
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
					const handler = broker.Promise.method(wf.handler).bind(svc);
					const handler2 = broker.middlewares.wrapHandler("localWorkflow", handler, wf);
					wf.handler = handler2;
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
						wf.handler = async (ctx: Context) => {
							const res = await check(ctx.params != null ? ctx.params : {});
							if (res === true) return handler3(ctx);
							else {
								throw new Errors.ValidationError(
									"Parameters validation error!",
									null,
									res
								);
							}
						};
					}
					const workflow = new Workflow(wf, svc);
					await workflow.init(broker, logger, mwOpts);
					svc.$workflows.push(workflow);
					logger.info(`Workflow '${workflow.name}' is registered.`);
				}
				// ...existing code for channel handler...
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
