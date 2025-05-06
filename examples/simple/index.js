/* eslint-disable no-console */
"use strict";

/**
 * It's a simple example which demonstrates how to
 * use the workflow middleware
 */

const { ServiceBroker } = require("moleculer");
const { MoleculerRetryableError } = require("moleculer").Errors;
const { inspect } = require("util");

const WorkFlowsMiddleware = require("../../index").Middleware;

let c = 1;

// Create broker
const broker = new ServiceBroker({
	logger: {
		type: "Console",
		options: {
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

	middlewares: [WorkFlowsMiddleware({})],

	replCommands: [
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
					option: "--cron <cron timing>",
					description: "Repeatable job with cron timing (e.g.: 15 3 * * * )"
				}
			],
			async action(broker, args) {
				const { options } = args;
				console.log(options);
				const jobOpts = {
					jobId: options.jobId,
					delay: options.delay,
					retries: options.retries != null ? parseInt(options.retries) : 0,
					timeout: options.timeout
				};

				if (options.cron) {
					jobOpts.repeat = { cron: options.cron /*endDate: "2025-05-06T19:25:00Z"*/ };
				}

				broker.wf.run(
					options.workflow || "test.wf1",
					{
						c: c++,
						name: "John",
						pid: process.pid,
						nodeID: broker.nodeID
					},
					jobOpts
				);
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
				const key = !Number.isNaN(Number(options.key)) ? Number(options.key) : options.key;
				broker.wf.adapter.triggerSignal(signalName, key, { user: "John Doe" });
			}
		},

		{
			command: "cleanup",
			alias: ["c"],
			async action(broker, args) {
				const { options } = args;
				//console.log(options);
				broker.wf.adapter.cleanUp("test.wf1");
			}
		}
	]
});

// Create a service
broker.createService({
	name: "test",

	// Define workflows
	workflows: {
		// User signup workflow.
		wf1: {
			// Workflow handler
			async handler(ctx) {
				this.logger.info("WF handler start", ctx.params, ctx.wf.jobId);
				/*

				const res = await ctx.call("test.list");
				await ctx.wf.setState("afterList");

				await ctx.emit("test.event");
				await ctx.wf.setState("afterEvent");

				const post = await ctx.wf.run("fetch", async () => {
					const res = await fetch("https://jsonplaceholder.typicode.com/posts/1");
					return await res.json();
				});
				await ctx.wf.setState("afterFetch");
				this.logger.info("Post result", post);

				for (let i = 0; i < 1; i++) {
					this.logger.info("Sleeping...");
					await ctx.wf.sleep(5000);
				}
				await ctx.wf.setState("afterSleep");

				//await ctx.call("test.danger", { name: "John Doe" });

				const signalRes = await ctx.wf.waitForSignal("test.signal", 123);
				this.logger.info("Signal result", signalRes);

				*/

				// await ctx.call("test.danger", { name: "John Doe" });

				this.logger.info("WF handler end", ctx.wf.jobId);

				return true;
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
