/* eslint-disable @/no-console */
"use strict";

/**
 * It's a simple example which demonstrates how to
 * use the workflow middleware
 */

import { ServiceBroker, Errors, Context } from "moleculer";
const { MoleculerRetryableError } = Errors;

import { inspect } from "node:util";
import process from "node:process";
import _ from "lodash";

import { Middleware } from "../../src/index.ts";
import { CreateJobOptions } from "../../src/types.ts";
import "../../src/moleculer-types.ts";
// import { Adapters } from "../../src/index.ts";

let c = 1;

let lastJobId;

// console.log(process.argv);
const isNoService = process.argv[2] === "noservice";

// Create broker
const broker = new ServiceBroker({
	transporter: "Redis",
	logger: {
		type: "Console",
		options: {
			formatter: "short",
			level: {
				WORKFLOWS: "debug",
				"*": "info"
			},
			objectPrinter: obj =>
				inspect(obj, {
					breakLength: 50,
					colors: true,
					depth: 3
				})
		}
	},

	metrics: {
		enabled: false,
		reporter: {
			type: "Console",
			options: {
				includes: ["moleculer.workflows.**"]
			}
		}
	},

	// Enable built-in tracing function. More info: https://moleculer.services/docs/0.14/tracing.html
	tracing: {
		enabled: true,
		// Available built-in exporters: "Console", "Datadog", "Event", "EventLegacy", "Jaeger", "Zipkin"
		exporter: {
			type: "Console", // Console exporter is only for development!
			options: {
				// Custom logger
				logger: null,
				// Using colors
				colors: true,
				// Width of row
				width: 100,
				// Gauge width in the row
				gaugeWidth: 40
			}
		}
	},

	middlewares: [
		Middleware({
			tracing: true
			//jobEventType: "broadcast"
		})
	],

	replOptions: {
		customCommands: [
			{
				command: "run",
				alias: ["r"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-j, --jobId <jobId>",
						description: "Job ID to run the workflow with"
					},
					{
						option: "-d, --delay <text_value_or_int>",
						description: "Delay before running (like: 5s, 1m, 6h, 2d)"
					},
					{
						option: "-r, --retries <count>",
						description: "Number of retry if failed"
					},
					{
						option: "-t, --timeout <text_value_or_int>",
						description: "Execution timeout (like: 5s, 1m, 6h, 2d)"
					},
					{
						option: "--cron <cron timing> [limit]",
						description: "Repeatable job with cron timing (e.g.: 15 3 * * * )"
					},
					{
						option: "--limit <limit>",
						description: "Number of executions"
					},
					{
						option: "--wait",
						description: "Wait for exection result"
					}
				],
				async action(broker, args) {
					const { options } = args;
					console.log(args);
					const jobOpts: CreateJobOptions = {
						jobId: options.jobId,
						delay: options.delay,
						retries: options.retries != null ? parseInt(options.retries) : undefined,
						timeout: options.timeout
					};

					if (options.cron) {
						jobOpts.repeat = {
							cron: options.cron /*endDate: "2025-05-06T19:25:00Z"*/,
							limit: options.limit != null ? Number(options.limit) : undefined
						};
					}

					const job = await broker.wf.run(
						options.workflow || "test.wf1",
						{
							c: c++,
							name: "John",
							pid: process.pid,
							nodeID: broker.nodeID
						},
						jobOpts
					);

					lastJobId = job.id;
					console.log("Job started", job);

					if (options.wait) {
						try {
							const res = await job.promise();
							console.log("Job result", res);
						} catch (err) {
							console.log("Job failed", err.message);
						}
					}
				}
			},

			{
				command: "batch",
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-c, --count <count>",
						description: "Number of jobs to run"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					const count = options.count || 100;

					const jobs = new Map();
					const start = Date.now();

					console.log(`Generate ${count} jobs...`);
					for (let i = 0; i < count; i++) {
						const jobId = "batch-" + i;
						const job = await broker.wf.run(
							options.workflow || "test.wf1",
							{ i },
							{ jobId }
						);

						job.promise().then(() => {
							jobs.delete(jobId);
							if (jobs.size === 0) {
								console.log(
									`All ${count} jobs finished. Time:`,
									Date.now() - start,
									"ms"
								);
							}
						});

						jobs.set(jobId, job);
					}
					console.log(`Generated ${count} jobs. Time:`, Date.now() - start, "ms");
				}
			},

			{
				command: "delete",
				alias: ["d"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-j, --jobId <jobId>",
						description: "Job ID to run the workflow with"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					return broker.wf.remove(
						options.workflow || "test.wf1",
						options.jobId || lastJobId
					);
				}
			},

			{
				command: "state",
				alias: ["t"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-j, --jobId <jobId>",
						description: "Job ID to run the workflow with"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					const state = await broker.wf.getState(
						options.workflow || "test.wf1",
						options.jobId || lastJobId
					);
					console.log("State", state);
				}
			},

			{
				command: "get",
				alias: ["g"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-j, --jobId <jobId>",
						description: "Job ID to run the workflow with"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					const job = await broker.wf.get(
						options.workflow || "test.wf1",
						options.jobId || lastJobId
					);
					console.log(job);
				}
			},

			{
				command: "jobEvents",
				alias: ["e"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					},
					{
						option: "-j, --jobId <jobId>",
						description: "Job ID to run the workflow with"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					const job = await broker.wf.getEvents(
						options.workflow || "test.wf1",
						options.jobId || lastJobId
					);
					console.log(job);
				}
			},

			{
				command: "signal [signalName]",
				alias: ["s"],
				options: [
					{
						option: "-k, --key <key>",
						description: "Signal key"
					}
				],
				async action(broker, args) {
					const { options } = args;
					// console.log(args);
					const signalName = options.signalName ?? "test.signal";
					const key = !Number.isNaN(Number(options.key))
						? Number(options.key)
						: options.key;
					broker.wf.triggerSignal(signalName, key, { user: "John Doe" });
				}
			},

			{
				command: "remsignal [signalName]",
				alias: ["z"],
				options: [
					{
						option: "-k, --key <key>",
						description: "Signal key"
					}
				],
				async action(broker, args) {
					const { options } = args;
					// console.log(args);
					const signalName = options.signalName ?? "test.signal";
					const key = !Number.isNaN(Number(options.key))
						? Number(options.key)
						: options.key;
					broker.wf.removeSignal(signalName, key);
				}
			},

			{
				command: "jobs [type]",
				alias: ["j"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					}
				],
				async action(broker, args, { table, kleur, getBorderCharacters }) {
					const { options } = args;
					//console.log(args);

					const tableConf = {
						border: _.mapValues(getBorderCharacters("honeywell"), char =>
							kleur.gray(char)
						),
						columns: {
							1: { alignment: "center" }
						},
						drawHorizontalLine: (index, count) =>
							index == 0 || index == 1 || index == count
					};

					const rows = [[kleur.bold("Job ID"), kleur.bold("Status"), kleur.bold("Time")]];

					if (!args.type || args.type == "active" || args.type == "live") {
						const active = await broker.wf.listActiveJobs(
							options.workflow || "test.wf1"
						);
						for (const item of active) {
							rows.push([item.id, kleur.magenta("Active"), ""]);
						}
					}
					if (!args.type || args.type == "delayed" || args.type == "live") {
						const delayed = await broker.wf.listDelayedJobs(
							options.workflow || "test.wf1"
						);
						for (const item of delayed) {
							rows.push([
								item.id,
								kleur.yellow("Delayed"),
								new Date(item.promoteAt).toLocaleTimeString()
							]);
						}
					}
					if (!args.type || args.type == "waiting" || args.type == "live") {
						const waiting = await broker.wf.listWaitingJobs(
							options.workflow || "test.wf1"
						);
						for (const jobId of waiting) {
							rows.push([jobId, kleur.cyan("Waiting"), ""]);
						}
					}

					if (!args.type || args.type == "completed" || args.type == "closed") {
						const completed = await broker.wf.listCompletedJobs(
							options.workflow || "test.wf1"
						);
						for (const item of completed) {
							rows.push([
								item.id,
								kleur.green("Completed"),
								new Date(item.finishedAt).toLocaleTimeString()
							]);
						}
					}
					if (!args.type || args.type == "failed" || args.type == "closed") {
						const failed = await broker.wf.listFailedJobs(
							options.workflow || "test.wf1"
						);
						for (const item of failed) {
							rows.push([
								item.id,
								kleur.red("Failed"),
								new Date(item.finishedAt).toLocaleTimeString()
							]);
						}
					}

					console.log(table(rows, tableConf));
				}
			},

			{
				command: "cleanup",
				alias: ["c"],
				options: [
					{
						option: "-w, --workflow <workflowName>",
						description: "Name of the workflow. Default: 'test.wf1'"
					}
				],
				async action(broker, args) {
					const { options } = args;
					//console.log(options);
					broker.wf.cleanUp(options.workflow ?? "test.wf1");
				}
			}
		]
	}
});

if (!isNoService) {
	// Create a service
	broker.createService({
		name: "test",

		// Define workflows
		workflows: {
			// Test 1 workflow.
			wf1: {
				concurrency: 3,
				maxStalledCount: 3,

				timeout: "5m",
				retention: "10m",

				retryPolicy: {
					retries: 3,
					delay: 1000,
					factor: 2
				},

				// @ts-expect-error skip param validation
				__params: {
					c: { type: "number" },
					name: { type: "string" },
					pid: { type: "number" },
					nodeID: { type: "string" },
					status: { type: "boolean" }
				},

				tracing: true,

				// Workflow handler
				async handler(ctx: Context) {
					this.logger.info("WF handler start", ctx.params, ctx.wf);

					const res = await ctx.call("test.list");
					await ctx.wf.setState("afterList");

					await ctx.emit("test.event");
					await ctx.wf.setState("afterEvent");

					const post = await ctx.wf.task("fetch", async () => {
						const res = await fetch("https://jsonplaceholder.typicode.com/posts/1");
						return await res.json();
					});
					await ctx.wf.setState("afterFetch");

					this.logger.info("Post result", post);

					// for (let i = 0; i < 5; i++) {
					// 	this.logger.info("Sleeping...");
					// 	await ctx.wf.sleep(1000);
					// 	await ctx.wf.setState("afterSleep-" + (i + 1));
					// }

					await ctx.wf.sleep("1s");
					// await ctx.wf.setState("afterSleep-20s");

					const signalRes = await ctx.wf.waitForSignal("test.signal", "123", {
						timeout: "1h"
					});
					this.logger.info("Signal result", signalRes);

					// await ctx.call("test.danger", { name: "John Doe" });

					this.logger.info("WF handler end", ctx.wf.jobId);

					await ctx.wf.setState({ progress: 100 });

					return post;
				}
			},

			wf2: {
				fullName: "wf2",
				enabled: false,
				concurrency: 10,
				async handler(ctx) {
					const job = await this.broker.wf.get(ctx.wf.name, ctx.wf.jobId);
					// console.log("Job", job);
					if (!job.retryAttempts || job.retryAttempts < 3) {
						throw new MoleculerRetryableError("Simulated failure");
					}
					return `Success on attempt ${job.retryAttempts}`;
				}
			}
		},

		actions: {
			list: {
				async handler(ctx) {
					// List something

					this.logger.info("List something");

					return [
						{ id: 1, name: "John Doe" },
						{ id: 2, name: "Jane Doe" },
						{ id: 3, name: "Jack Doe" }
					];
				}
			},

			danger(ctx) {
				this.logger.info("Danger action", ctx.params);
				if (Math.random() > 0.5) {
					throw new MoleculerRetryableError("Random error");
				}
				return { ok: true };
			}
		},

		events: {
			"test.event": {
				async handler(ctx) {
					this.logger.info("Test Event handler", ctx.params);
				}
			}
		}
	});
} else {
	/*broker.createService({
		name: "job-events",

		events: {
			"job.**": {
				async handler(ctx) {
					this.logger.info(">>> JOB EVENT ", ctx.eventName, ctx.params.job);
				}
			}
		}
	});*/
}

// Start server
broker
	.start()
	.then(async () => {
		// broker.wf.run("test.wf1", { name: "John Doe" }, { jobId: "1111" });
	})
	.then(() => broker.repl())
	.catch(err => {
		broker.logger.error(err);
		process.exit(1);
	});
