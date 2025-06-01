/* eslint-disable @typescript-eslint/no-require-imports */

"use strict";

const { ServiceBroker } = require("moleculer");
const { Middleware } = require("../../dist/cjs/index.js");

// Create broker
const broker = new ServiceBroker({
	middlewares: [Middleware({})]
});

// Create a service
broker.createService({
	name: "users",

	// Define workflows
	workflows: {
		// User signup workflow.
		test: {
			async handler(ctx) {
				this.logger.info("Running test workflow with params:", ctx.params);
				return `Test ${ctx.params.name}`;
			}
		}
	}
});

// Start server
broker
	.start()
	.then(async () => {
		const job = await broker.wf.run("users.test", { name: "John Doe" });
		const result = await job.promise();
		broker.logger.info("Workflow completed with result:", result);
	})
	.then(() => broker.repl())
	.catch(err => {
		broker.logger.error(err);
		process.exit(1);
	});
