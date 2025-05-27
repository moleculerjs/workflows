/* eslint-disable @/no-console */
"use strict";

/**
 * It's a full example which demonstrates how to
 * use the workflow middleware
 */

import { ServiceBroker } from "moleculer";
import { Errors } from "moleculer";
const { MoleculerClientError } = Errors;
import ApiGateway from "moleculer-web";

import Middleware from "../../dist";

// Create broker
const broker = new ServiceBroker({
	metrics: {
		enabled: false,
		reporter: {
			type: "Console",
			options: {
				includes: ["moleculer.workflows.**"]
			}
		}
	},

	tracing: {
		enabled: false,
		exporter: {
			type: "Console"
		}
	},

	middlewares: [Middleware({})]
});

broker.createService({
	name: "api",
	mixins: [ApiGateway],
	settings: {
		port: 3003,
		routes: [
			{
				path: "/api",
				aliases: {
					"POST /register": "users.signup",
					"POST /verify/:token": "users.verify",
					"GET /state/:jobId": "users.checkSignupState"
				}
			}
		]
	}
});

const USERS = [];

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
				const isExist = await ctx.call("users.getByEmail", { email: ctx.params.email });

				if (isExist) {
					throw new MoleculerClientError(
						"E-mail address is already signed up. Use the login button"
					);
				}

				// Check the e-mail address is valid and not a temp mail address
				await ctx.call("utils.isTemporaryEmail", { email: ctx.params.email });

				// Register (max execution is 10 sec)
				const user = await ctx.call("users.register", ctx.params, {
					timeout: 10,
					// For Saga, define the compensation action in case of failure
					compensation: "users.remove"
				});

				// Send verification
				await ctx.call("mail.send", { type: "verification", user });

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
						await ctx.call("user.remove", { id: user.id });
						return null;
					}

					// Other error is thrown further
					throw err;
				}

				// Set user verified and save
				user.verified = true;
				await ctx.call("users.update", user);

				// Send event to Moleculer services
				await ctx.broadcast("user.registered", user);

				// Other non-moleculer related workflow task
				await ctx.wf.task("httpPost", async () => {
					await fetch("https://jsonplaceholder.typicode.com/posts/1", { method: "GET" });
				});

				// Send welcome email
				await ctx.call("mail.send", { type: "welcome", user });

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
			params: {
				email: { type: "string", min: 3, max: 100 }
			},
			async handler(ctx) {
				const job = await this.broker.wf.run("users.signupWorkflow", ctx.params, {
					jobId: ctx.requestID // optional
					/* other workflow run options */
				});
				// Here the workflow is running, the res is a state object
				return {
					// With the jobId, you can call the `checkSignupState` REST action
					// to get the state of the execution on the frontend.
					jobId: job.id
				};

				// or wait for the execution and return the result
				// return await job.promise();
			}
		},

		verify: {
			rest: "POST /verify/:token",
			async handler(ctx) {
				// Check the validity
				const user = await ctx.call("users.findByToken", {
					verificationToken: ctx.params.token
				});
				if (user) {
					await this.broker.wf.triggerSignal("email.verification", user.id, { a: 5 });
				}
			}
		},

		checkSignupState: {
			rest: "GET /state/:jobId",
			params: {
				jobId: { type: "string" }
			},
			async handler(ctx) {
				return await this.broker.wf.getState("users.signupWorkflow", ctx.params.jobId);
			}
		},

		getByEmail: {
			async handler(ctx) {
				return USERS.find(user => user.email === ctx.params.email);
			}
		},

		register: {
			async handler(ctx) {
				const user = { id: USERS.length + 1, email: ctx.params.email };
				user.verificationToken = "token_" + user.id;
				user.verified = false;
				USERS.push(user);
				return user;
			}
		},

		update: {
			async handler(ctx) {
				const user = USERS.find(user => user.id === ctx.params.id);
				if (user) {
					Object.assign(user, ctx.params);
				}
				return user;
			}
		},

		findByToken: {
			async handler(ctx) {
				const user = USERS.find(
					user => user.verificationToken === ctx.params.verificationToken
				);
				if (!user) {
					throw new MoleculerClientError("Invalid token", 422, "INVALID_TOKEN");
				}
				return user;
			}
		}
	},

	events: {
		"user.registered": {
			handler(payload) {
				console.log("User registered", payload);
			}
		}
	},

	started() {
		// this.broker.wf.run("notifyNonLoggedUsers", {}, {
		// 	jobId: "midnight-notify", // Only start a new schedule if not exists with the same jobId
		// 	// Delayed run
		// 	delay: "1 hour",
		// 	// Recurring run
		// 	repeat: {
		// 		cron: "0 0 * * *" // run every midnight
		// 	}
		// });
	}
});

broker.createService({
	name: "mail",
	actions: {
		send(ctx) {
			console.log(
				`Send ${ctx.params.type} mail to ${ctx.params.user.email}, token: ${ctx.params.user.verificationToken}`
			);

			return true;
		}
	}
});
broker.createService({
	name: "utils",
	actions: {
		isTemporaryEmail(ctx) {
			console.log("Check if the email is temporary", ctx.params.email);
			return false;
		}
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
