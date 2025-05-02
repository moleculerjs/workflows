/* eslint-disable no-console */
"use strict";

/**
 * It's a simple example which demonstrates how to
 * use the workflow middleware
 */

const { ServiceBroker } = require("moleculer");
const { MoleculerClientError } = require("moleculer").Errors;
const { inspect } = require("util");

const WorkFlowsMiddleware = require("../../index").Middleware;

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
	metrics: {
		enabled: false,
		reporter: {
			type: "Console",
			options: {
				includes: ["moleculer.workflow.**"]
			}
		}
	},

	tracing: {
		enabled: false,
		exporter: {
			type: "Console"
		}
	},

	middlewares: [WorkFlowsMiddleware({})]
});

// Create a service
broker.createService({
	name: "users",

	// Define workflows
	workflows: {
		// User signup workflow.
		signupWorkflow: {
			//  Workflow execution timeout
			timeout: "1 day",

			// Workflow event history retention
			retention: "3 days",

			// Retry policy
			retryPolicy: {},

			// Concurrent running jobs
			concurrency: 3,

			// Parameter validation
			params: {},

			// Workflow handler
			async handler(ctx) {
				// Check the e-mail address is not exists
				const isExist = await ctx.wf.call("users.getByEmail", { email: ctx.params.email });

				if (isExist) {
					throw new MoleculerClientError(
						"E-mail address is already signed up. Use the login button"
					);
				}

				// Check the e-mail address is valid and not a temp mail address
				await ctx.wf.call("utils.isTemporaryEmail", { email: ctx.params.email });

				// Register (max execution is 10 sec)
				const user = await ctx.wf.call("users.register", ctx.params, {
					timeout: 10,
					// Set the workflow state to this before call the action
					beforeSetState: "REGISTERING",
					// Set the workflow state to this after the action called
					afterSetState: "SENDING_EMAIL",
					// For Saga, define the compensation action in case of failure
					compensation: "users.remove"
				});

				// Send verification
				await ctx.wf.call("mail.send", { type: "verification", user });

				// Wait for verification (max 1 hour)
				await ctx.wf.setState("WAIT_VERIFICATION");

				try {
					await ctx.wf.waitForSignal("email.verification", user.id, {
						timeout: "1 hour"
					});
					await ctx.wf.setState("VERIFIED");
				} catch (err) {
					if (err.name == "WorkflowTaskTimeoutError") {
						// Registraion not verified in 1 hour, remove the user
						await ctx.wf.call("user.remove", { id: user.id });
						return null;
					}

					// Other error is thrown further
					throw err;
				}

				// Set user verified and save
				user.verified = true;
				await ctx.wf.call("users.update", user);

				// Send event to Moleculer services
				await ctx.wf.broadcast("user.registered", user);

				// Other non-moleculer related workflow task
				await ctx.wf.run("httpPost", async () => {
					await fetch("https://...", { method: "POST", data: "" });
				});

				// Send welcome email
				await ctx.wf.call("mail.send", { type: "welcome", user });

				// Set the workflow state to done (It can be a string, number, or object)
				await ctx.wf.setState("DONE");

				// It will be stored as a result value to the workflow in event history
				return user;
			}
		}
	},

	actions: {
		signup: {
			rest: "POST /register",
			async handler(ctx) {
				const res = await ctx.wf.run("users.signupWorkflow", ctx.params, {
					workflowId: ctx.requestID // optional
					/* other workflow run options */
				});
				// Here the workflow is running, the res is a state object
				return {
					// With the workflowId, you can call the `checkSignupState` REST action
					// to get the state of the execution on the frontend.
					workflowId: res.workflowId
				};

				// or wait for the execution and return the result
				// return await res.result();
			}
		},

		verify: {
			rest: "POST /verify/:token",
			async handler(ctx) {
				// Check the validity
				const user = ctx.call("users.find", { verificationToken: ctx.params.token });
				if (user) {
					ctx.wf.sendSignal("email.verification", user.id, { a: 5 });
				}
			}
		},

		checkSignupState: {
			rest: "GET /state/:workflowId",
			async handler(ctx) {
				const res = await ctx.wf.getState({ workflowId: ctx.params.workflowId });
				if (res.state == "DONE") {
					return { user: res.result };
				} else {
					return { state: res.state, startedAt: res.startedAt, duration: res.duration };
				}
			}
		}
	},

	started() {
		// this.broker.wf.run("notifyNonLoggedUsers", {}, {
		// 	workflowId: "midnight-notify", // Only start a new schedule if not exists with the same workflowId
		// 	// Delayed run
		// 	startDelay: "1 hour",
		// 	// Recurring run
		// 	schedule: {
		// 		cron: "0 0 * * *" // run every midnight
		// 	}
		// });
	}
});

// Start server
broker
	.start()
	.then(async () => {})
	.then(() => broker.repl())
	.catch(err => {
		broker.logger.error(err);
		process.exit(1);
	});
