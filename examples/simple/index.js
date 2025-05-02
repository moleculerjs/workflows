/* eslint-disable no-console */
"use strict";

/**
 * It's a simple example which demonstrates how to
 * use the workflow middleware
 */

const { ServiceBroker } = require("moleculer");
// const { MoleculerClientError } = require("moleculer").Errors;
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
					option: "-j, --jobId <jobId>",
					description: "Job ID to run the workflow with"
				}
			],
			async action(broker, args) {
				const { options } = args;
				console.log(options);
				broker.wf.run(
					"test.wf1",
					{
						c: c++,
						name: "John",
						pid: process.pid,
						nodeID: broker.nodeID
					},
					{ jobId: options.jobId }
				);
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

				const res = await ctx.call("test.list");

				await ctx.wf.sleep(5000);

				this.logger.info("WF handler end", ctx.wf.jobId);

				return res;
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
