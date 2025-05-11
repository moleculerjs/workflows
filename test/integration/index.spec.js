const { ServiceBroker } = require("moleculer");
const { MoleculerRetryableError } = require("moleculer").Errors;
const WorkflowsMiddleware = require("../../src");

describe("Workflows Middleware Integration Test", () => {
	let broker;

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "test",
			workflows: {
				simpleWorkflow: {
					async handler(ctx) {
						return `Hello, ${ctx.params.name}`;
					}
				},
				errorWorkflow: {
					async handler(ctx) {
						throw new Error("Workflow execution failed");
					}
				},
				longWorkflow: {
					async handler(ctx) {
						await new Promise(resolve => setTimeout(resolve, 1000));
						return `Processed ${ctx.params.id}`;
					}
				},
				contextWorkflow: {
					async handler(ctx) {
						return {
							user: ctx.user,
							meta: ctx.meta,
							params: ctx.params
						};
					}
				}
			}
		});

		await broker.start();
	});

	afterAll(async () => {
		await broker.wf.cleanup("test.simpleWorkflow");
		await broker.wf.cleanup("test.errorWorkflow");
		await broker.wf.cleanup("test.longWorkflow");
		await broker.wf.cleanup("test.contextWorkflow");
		await broker.wf.cleanup("retryTest.retryWorkflow");
		await broker.stop();
	});

	it("should execute a simple workflow and return the expected result", async () => {
		const job = await broker.wf.run("test.simpleWorkflow", { name: "World" });
		expect(job.id).toEqual(expect.any(String));
		expect(job.payload).toStrictEqual({ name: "World" });

		const result = await job.promise();
		expect(result).toBe("Hello, World");
	});

	it("should handle workflow errors correctly", async () => {
		const job = await broker.wf.run("test.errorWorkflow", { name: "Error" });

		await expect(job.promise()).rejects.toThrow("Workflow execution failed");
	});

	it("should handle concurrent workflows", async () => {
		const promises = [];
		for (let i = 0; i < 5; i++) {
			promises.push((await broker.wf.run("test.longWorkflow", { id: i })).promise());
		}

		const results = await Promise.all(promises);
		expect(results.length).toBe(5);
		results.forEach((result, index) => {
			expect(result).toBe(`Processed ${index}`);
		});
	}, 15000);

	describe("Retries Feature", () => {
		it("should retry the workflow handler on failure", async () => {
			let attempt = 0;

			// Add a new service with a handler that fails the first two times
			broker.createService({
				name: "retryTest",
				workflows: {
					retryWorkflow: {
						async handler() {
							attempt++;
							if (attempt < 3) {
								throw new MoleculerRetryableError("Simulated failure");
							}
							return `Success on attempt ${attempt}`;
						}
					}
				}
			});

			// Run the workflow and verify the result
			const job = await broker.wf.run("retryTest.retryWorkflow", {}, { retries: 2 });
			expect(job.id).toEqual(expect.any(String));
			const result = await job.promise();
			expect(result).toBe("Success on attempt 3");
		}, 30000);
	});
});
