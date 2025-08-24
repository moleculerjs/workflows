import { describe, expect, it, beforeAll, afterAll } from "vitest";
import { ServiceBroker } from "moleculer";
import WorkflowsMiddleware from "../../src/middleware.ts";
import "../vitest-extensions.ts";

describe("Fake Adapter Integration Test", () => {
	let broker: ServiceBroker;

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Fake" })]
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
	});

	afterAll(async () => {
		if (broker) {
			await broker.wf.cleanUp();
			await broker.stop();
		}
	});

	it("should execute a simple workflow with Fake adapter", async () => {
		const job = await broker.wf.run("test.simple", { name: "fake-test" });

		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "fake-test" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, fake-test");
	}, 5000);

	it("should handle workflow state operations", async () => {
		const job = await broker.wf.run("test.simple", { name: "state-test" });

		// Get job state
		const state = await broker.wf.getState("test.simple", job.id);
		expect(state).toBeDefined();

		// Get job details
		const jobDetails = await broker.wf.get("test.simple", job.id);
		expect(jobDetails).toBeDefined();
		expect(jobDetails.id).toBe(job.id);

		await job.promise();
	}, 10000);
});
