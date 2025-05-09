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
				}
			}
		});

		await broker.start();
	});

	afterAll(async () => {
		await broker.stop();
	});

	it("should execute a simple workflow and return the expected result", async () => {
		const result = await broker.wf.run("test.simpleWorkflow", { name: "World" });
		expect(result.id).toEqual(expect.any(String));
		expect(result.payload).toStrictEqual({ name: "World" });
	});

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
