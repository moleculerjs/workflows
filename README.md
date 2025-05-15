![Moleculer logo](http://moleculer.services/images/banner.png)

![Integration Test](https://github.com/moleculerjs/workflows/workflows/Integration%20Test/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/moleculerjs/workflows/badge.svg?branch=master)](https://coveralls.io/github/moleculerjs/workflows?branch=master)
[![Known Vulnerabilities](https://snyk.io/test/github/moleculerjs/workflows/badge.svg)](https://snyk.io/test/github/moleculerjs/workflows)
[![NPM version](https://badgen.net/npm/v/@moleculer/workflows)](https://www.npmjs.com/package/@moleculer/workflows)

# @moleculer/workflows
Reliable & scalable workflow feature (like Temporal.io or Restate) for Moleculer framework.

**This project is in work-in-progress. Be careful using it in production.**

## Features

- Reliable and scalable workflow management for the Moleculer framework.
- Supports multiple adapters (e.g., Redis, Fake) for workflow storage.
- Workflow execution with concurrency control and retry policies.
- Event-driven architecture with support for signals and state transitions.
- Built-in metrics and monitoring capabilities.
- Parameter validation and error handling.
- Workflow history retention and maintenance.
- Integration with Moleculer services and actions.

## Install

To install the package, use the following command:

```bash
npm i @moleculer/workflows
```

## Usage

### Basic Example

```javascript
const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("@moleculer/workflows").Middleware;

// Create a ServiceBroker
const broker = new ServiceBroker({
    logger: true,
    middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
});

// Define a service with workflows
broker.createService({
    name: "test",
    workflows: {
        simpleWorkflow: {
            async handler(ctx) {
                return `Hello, ${ctx.params.name}`;
            }
        }
    }
});

// Start the broker
broker.start().then(async () => {
    // Run the workflow
    const result = await broker.wf.run("test.simpleWorkflow", { name: "World" });
    console.log(result);
});
```

### Advanced Example (User sign-up workflow)

```javascript
broker.createService({
    name: "users",
    workflows: {
        signupWorkflow: {
            // Max execution time
            timeout: "1 day",
            // Retention of finished job history
            retention: "3 days",
            // Concurrent executions
            concurrency: 3,
            // Parameter validation
            params: {
                email: { type: "email" },
                name: { type: "string" }
            },
            async handler(ctx) {
                // Create user
                const user = await ctx.call("users.create", ctx.params);
                
                // Save the state of the job (It can be a string, number, or object)
                await ctx.wf.setState("CREATED");

                // Send verification email
                await ctx.call("mail.send", { type: "welcome", user });

                try {
                    // Waiting for verification (max 1h)
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

                    // Other error should be thrown further
                    throw err;
                }

                // Set user verified and save
                user.verified = true;
                await ctx.call("users.update", user);
                
                // Set the workflow state to done
                await ctx.wf.setState("DONE");

                return user;
            }
        }
    },
    actions: {
        // ... common actions

        // REST API to start the signup workflow job
		register: {
			rest: "POST /register",
			async handler(ctx) {
				const job = await this.broker.wf.run("users.signupWorkflow", ctx.params, {
					jobId: ctx.requestID // optional
					/* other options */
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

        // REST API for verification URL in the sent e-mail
		verify: {
			rest: "POST /verify/:token",
			async handler(ctx) {
				// Check the validity
				const user = ctx.call("users.find", { verificationToken: ctx.params.token });
				if (user) {
                    // Trigger the signal for the Workflow job to continue the execution
					this.broker.wf.triggerSignal("email.verification", user.id);
				}
			}
		},

        // Check the signup process state. You can call it from the frontend
        // and show the current state for the customer. The `jobId` sent back 
        // in the "register" REST call.
		checkRegisterState: {
			rest: "GET /registerState/:jobId",
			async handler(ctx) {
				const res = await ctx.wf.getState({ jobId: ctx.params.jobId });
				if (res.state == "DONE") {
					return { state: res.state, user: res.result };
				} else {
					return { state: res.state, startedAt: res.startedAt };
				}
			}
		}
    }
});
```

## Options

### WorkflowsMiddleware (Mixin) Options

| Name                  | Type                                                      | Description                                                                                 |
|-----------------------|-----------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `adapter`               | `string` \| `BaseAdapter` \| `RedisAdapterOptions`           | Adapter instance, name, or options for workflow storage. **Default:** `"Redis"`                                    |
| `schemaProperty`        | `string`                                                    | Service schema property name for workflows. **Default:** `"workflows"`                                                 |
| `workflowHandlerTrigger`| `string`                                                    | Name of the method to trigger workflow handler. **Default:** `emitLocalWorkflowHandler`                                 |
| `jobEventType`          | `string`                                                    | How job events are emitted (e.g., `broadcast, `emit`).                                 |
| `lockExpiration` | `number`                                                    | Job lock expiration time (sec). **Default:** `30`                                                     |

### RedisAdapter Options

| Name         | Type                                                      | Description                                                                                 |
|--------------|-----------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `redis`        | `RedisOptions` \| `{ url: string }` \| `{ cluster: { nodes: string[]; clusterOptions?: any } }` | Redis connection options, URL, or cluster configuration. **Default:** (required)                                   |
| `prefix`       | `string`                                                    | Prefix for Redis keys. **Default:** `"wf"`                                                                     |
| `serializer`   | `string`                                                    | Serializer to use for job data. **Default:** `JSON`                                                            |
| `drainDelay`   | `number`                                                    | Blocking delay time (sec). **Default:** `5`                                                          |

### Workflow options
| Name         | Type   | Description  |
|--------------|--------|-------------|
| `name`        | `String`  | Name of workflow. **Default:** service name + workflow name               |
| `fullName`        | `String`  | Full name of workflow. If you don't want to prepend the service name for the workflow.name.               |
| `params`        | `object` | Job parameter validation schema **Default:** optional              |
| `removeOnCompleted`        | `boolean`  | Remove the job when it's completed. **Default:** `false`      |
| `removeOnFailed`        | `boolean`  | Remove the job when it's failed. **Default:** `false`      |
| `concurrency`        | `number`  | Number of concurrent running jobs. **Default:** `1`       |
| `retention`        | `string` or `number`  | Retention time of job history. **Default:** `null`      |
| `backoff`        | `string`\|`Function`  | Retry backoff strategy. Available: `fixed`, `exponention` or a custom Function **Default:** `fixed`      |
| `backoffDelay`        | `number`  | Backoff delay for fixed and exponential strategy. **Default:** `100`      |
| `maxStalledCount`        | `number`  | Number of maximum put back the stalled job. `0` or `null` value disables it. **Default:** `null`      |

## License
The project is available under the [MIT license](https://tldrlegal.com/license/mit-license).

## Contact
Copyright (c) 2025 MoleculerJS

[![@MoleculerJS](https://img.shields.io/badge/github-moleculerjs-green.svg)](https://github.com/moleculerjs) [![@MoleculerJS](https://img.shields.io/badge/twitter-MoleculerJS-blue.svg)](https://twitter.com/MoleculerJS)
