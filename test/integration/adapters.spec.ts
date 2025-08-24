import { describe, expect, it, afterAll } from "vitest";

import { ServiceBroker } from "moleculer";
import WorkflowsMiddleware from "../../src/middleware.ts";

import "../vitest-extensions.ts";
import { Adapters } from "../../src";

describe("Workflows Adapters Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("adapters.simple");
	};

	async function createBroker(adapter?) {
		if (broker) {
			cleanup();

			broker.stop();
			broker = null;
		}

		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter })]
		});

		broker.createService({
			name: "test",
			workflows: {
				simple: {
					async handler(ctx) {
						return `Hello, ${ctx.params?.name}`;
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	}

	afterAll(async () => {
		await cleanup();
		await broker.stop();
	});

	it("should work with Fake adapter", async () => {
		await createBroker("Fake");

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with Fake adapter object definition", async () => {
		await createBroker({ type: "Fake" });

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with Fake adapter with options", async () => {
		await createBroker({
			type: "Fake",
			options: { drainDelay: 2 }
		});

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work without adapter definition", async () => {
		await createBroker();

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with URI definition", async () => {
		await createBroker("redis://localhost:6379");

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with object definition", async () => {
		await createBroker({ type: "Redis" });

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with object definition (with options)", async () => {
		await createBroker({
			type: "Redis",
			options: { redis: { host: "localhost", port: 6379 } }
		});

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with adapter instance", async () => {
		await createBroker({ type: Adapters.Redis });

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});

	it("should work with adapter instance (with options)", async () => {
		await createBroker({
			type: Adapters.Redis,
			options: { redis: { host: "localhost", port: 6379 } }
		});

		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, ephemeral");
	});
});
