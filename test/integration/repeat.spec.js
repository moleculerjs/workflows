const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");

describe("Workflows Repeat Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("repeat.doWorkflow");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "repeat",
			workflows: {
				doWorkflow: {
					async handler(ctx) {
						return `Hello, it's called`;
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

	/*
		TODO:
		- cron repeat
		- limit exections
		- endDate

	*/

	it("should execute a simple workflow and return the expected result", async () => {
		// const job = await broker.wf.run("test.simpleWorkflow", { name: "World" });
		// expect(job.id).toEqual(expect.any(String));
		// expect(job.payload).toStrictEqual({ name: "World" });
		// const result = await job.promise();
		// expect(result).toBe("Hello, World");
	});
});
