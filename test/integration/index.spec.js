const { ServiceBroker } = require("moleculer");
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
});
