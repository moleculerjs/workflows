import { describe, expect, it, beforeAll, afterAll } from "vitest";

import { ServiceBroker } from "moleculer";
import WorkflowsMiddleware from "../../src/middleware.ts";

import "../vitest-extensions.ts";

describe("Workflows Delayed Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("delayed.simple");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "delayed",
			workflows: {
				simple: {
					async handler() {
						return `Hello, it's called`;
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		await (await broker.wf.getAdapter()).dumpWorkflows("./tmp", ["delayed.simple"]);
		await cleanup();
		await broker.stop();
	});

	it("should execute a delayed job", async () => {
		const now = Date.now();
		const job = await broker.wf.run("delayed.simple", { name: "John" }, { delay: "8s" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			delay: 8_000,
			payload: { name: "John" },
			promoteAt: expect.greaterThanOrEqual(now + 8_000),
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe(`Hello, it's called`);

		const job2 = await broker.wf.get("delayed.simple", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			delay: 8_000,
			promoteAt: expect.greaterThanOrEqual(now + 8_000),
			startedAt: expect.greaterThanOrEqual(job2.promoteAt),
			finishedAt: expect.greaterThanOrEqual(job2.startedAt),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: `Hello, it's called`
		});
	}, 10_000);

	it("should execute two delayed job (no wait for maintenance time)", async () => {
		const now = Date.now();
		const job1 = await broker.wf.run("delayed.simple", { name: "John" }, { delay: "6s" });
		const job2 = await broker.wf.run("delayed.simple", { name: "John" }, { delay: "5s" });

		const result2 = await job2.promise();
		expect(result2).toBe(`Hello, it's called`);

		const job22 = await broker.wf.get("delayed.simple", job2.id);
		expect(job22).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			delay: 5000,
			promoteAt: expect.greaterThanOrEqual(now + 5000),
			startedAt: expect.greaterThanOrEqual(job22.promoteAt),
			finishedAt: expect.greaterThanOrEqual(job22.startedAt),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: `Hello, it's called`
		});

		const result1 = await job1.promise();
		expect(result1).toBe(`Hello, it's called`);

		const job11 = await broker.wf.get("delayed.simple", job1.id);
		expect(job11).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			delay: 6000,
			promoteAt: expect.greaterThanOrEqual(now + 6000),
			startedAt: expect.greaterThanOrEqual(job11.promoteAt),
			finishedAt: expect.greaterThanOrEqual(job11.startedAt),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: `Hello, it's called`
		});

		expect(job11.startedAt - job22.startedAt).withinRange(800, 1200);
	}, 10_000);
});
