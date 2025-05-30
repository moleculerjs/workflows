/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

import _ from "lodash";
import { Utils } from "moleculer";
const { isFunction, isPlainObject, safetyObject } = Utils;

import type { ServiceBroker, Tracer } from "moleculer";
import type { WorkflowOptions } from "./workflow.ts";
import { WorkflowHandler } from "./types.ts";

export default function tracingLocalChannelMiddleware(
	handler: WorkflowHandler,
	wf: WorkflowOptions
): WorkflowHandler {
	let opts: Exclude<WorkflowOptions["tracing"], boolean>;
	if (wf.tracing === true || wf.tracing === false) opts = { enabled: !!wf.tracing };
	else opts = _.defaultsDeep({}, opts, { enabled: true });

	// eslint-disable-next-line @typescript-eslint/no-this-alias
	const broker: ServiceBroker = this;
	const tracer: Tracer = this.tracer;

	if (broker.isTracingEnabled() && opts.enabled) {
		return function tracingLocalChannelMiddleware(ctx) {
			ctx.requestID = ctx.requestID || tracer.getCurrentTraceID();
			ctx.parentID = ctx.parentID || tracer.getActiveSpanID();

			let tags: Record<string, unknown> = {
				callingLevel: ctx.level,
				workflow: {
					name: ctx.wf.name,
					jobId: ctx.wf.jobId
				},
				remoteCall: ctx.nodeID !== broker.nodeID,
				callerNodeID: ctx.nodeID,
				nodeID: broker.nodeID,
				/*options: {
						timeout: ctx.options.timeout,
						retries: ctx.options.retries
					},*/
				requestID: ctx.requestID
			};

			let actionTags: Record<string, unknown>;
			// local action tags take precedence
			if (isFunction(opts.tags)) {
				actionTags = opts.tags();
			} else {
				// By default all params are captured. This can be overridden globally and locally
				actionTags = { ...{ params: true }, ...opts.tags };
			}

			if (isFunction(actionTags)) {
				const res = actionTags.call(ctx.service, ctx);
				if (res) Object.assign(tags, res);
			} else if (isPlainObject(actionTags)) {
				if (actionTags.params === true)
					tags.params =
						ctx.params != null && isPlainObject(ctx.params)
							? Object.assign({}, ctx.params)
							: ctx.params;
				else if (Array.isArray(actionTags.params))
					tags.params = _.pick(ctx.params, actionTags.params);

				if (actionTags.meta === true)
					tags.meta = ctx.meta != null ? Object.assign({}, ctx.meta) : ctx.meta;
				else if (Array.isArray(actionTags.meta))
					tags.meta = _.pick(ctx.meta, actionTags.meta);
			}

			if (opts.safetyTags) {
				tags = safetyObject(tags);
			}

			let spanName = `workflow '${wf.name}'`;
			if (opts.spanName) {
				switch (typeof opts.spanName) {
					case "string":
						spanName = opts.spanName;
						break;
					case "function":
						spanName = opts.spanName.call(ctx.service, ctx);
						break;
				}
			}

			const span = ctx.startSpan(spanName, {
				id: ctx.id,
				type: "workflow",
				traceID: ctx.requestID,
				parentID: ctx.parentID,
				service: ctx.service,
				sampled: ctx.tracing,
				tags
			});

			ctx.tracing = span.sampled;

			// Call the handler
			return handler(ctx)
				.then(res => {
					ctx.finishSpan(span);
					return res;
				})
				.catch(err => {
					span.setError(err);
					ctx.finishSpan(span);
					throw err;
				});
		}.bind(this);
	}

	return handler;
}
