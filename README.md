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

### Advanced Example

```javascript
broker.createService({
    name: "users",
    workflows: {
        signupWorkflow: {
            timeout: "1 day",
            retention: "3 days",
            concurrency: 3,
            async handler(ctx) {
                const user = await ctx.call("users.register", ctx.params);
                await ctx.wf.setState("REGISTERED");
                await ctx.call("mail.send", { type: "welcome", user });
                return user;
            }
        }
    },
    actions: {
        register(ctx) {
            // User registration logic
        },
        sendMail(ctx) {
            // Email sending logic
        }
    }
});
```

## Options

### WorkflowsMiddleware (Mixin) Options

| Name                  | Type                                                      | Default         | Description                                                                                 |
|-----------------------|-----------------------------------------------------------|-----------------|---------------------------------------------------------------------------------------------|
| adapter               | string \| BaseAdapter \| RedisAdapterOptions           | (required)      | Adapter instance, name, or options for workflow storage.                                    |
| schemaProperty        | string                                                    | "workflows"    | Service schema property name for workflows.                                                  |
| workflowHandlerTrigger| string                                                    | "emitLocalWorkflowHandler" | Name of the method to trigger workflow handler.                                 |
| jobEventType          | string                                                    |                 | How job events are emitted (e.g., "broadcast", "emit").                                   |

### RedisAdapter Options

| Name         | Type                                                      | Default     | Description                                                                                 |
|--------------|-----------------------------------------------------------|-------------|---------------------------------------------------------------------------------------------|
| redis        | RedisOptions \| { url: string } \| { cluster: { nodes: string[]; clusterOptions?: any } } | (required)  | Redis connection options, URL, or cluster configuration.                                    |
| prefix       | string                                                    | "wf"       | Prefix for Redis keys.                                                                      |
| serializer   | string                                                    | "JSON"     | Serializer to use for job data.                                                             |
| drainDelay   | number                                                    | 5         | Blocking delay time (sec).                                                           |
| lockDuration | number                                                    | 30000       | Lock duration (ms) for job processing.                                                      |

<!-- ## Documentation
You can find [here the documentation](docs/README.md).

## Benchmark
There is some benchmark with all adapters. [You can find the results here.](benchmark/results/common/README.md) -->

## License
The project is available under the [MIT license](https://tldrlegal.com/license/mit-license).

## Contact
Copyright (c) 2025 MoleculerJS

[![@MoleculerJS](https://img.shields.io/badge/github-moleculerjs-green.svg)](https://github.com/moleculerjs) [![@MoleculerJS](https://img.shields.io/badge/twitter-MoleculerJS-blue.svg)](https://twitter.com/MoleculerJS)
