const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { MoleculerRetryableError } = require("moleculer").Errors;

describe("Workflows Retries Test", () => {
	let broker;
	let attempt = 0;

	const cleanup = async () => {
		await broker.wf.cleanup("retry.simple");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "retry",
			workflows: {
				simple: {
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

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		await cleanup();
		await broker.stop();
	});

	it("should retry the workflow handler on failure", async () => {
		const job = await broker.wf.run("retry.simple", {}, { retries: 2 });
		expect(job.id).toEqual(expect.any(String));
		const result = await job.promise();
		expect(result).toBe("Success on attempt 3");
	}, 30000);
});
