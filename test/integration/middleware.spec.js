const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Middleware Test", () => {
	let broker, worker;
	let FLOWS = [];

	const cleanup = async () => {
		await broker.wf.cleanUp("mw.good");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		worker = new ServiceBroker({
			logger: false,
			middlewares: [
				WorkflowsMiddleware({ adapter: "Redis" }),
				{
					name: "test",
					localWorkflow(next) {
						return async function (ctx) {
							FLOWS.push("BEFORE -> " + ctx.wf.jobId + " -> " + ctx.params.name);
							const result = await next(ctx);
							FLOWS.push("AFTER -> " + ctx.wf.jobId + " -> " + result);
							return result;
						};
					}
				}
			]
		});

		worker.createService({
			name: "mw",
			workflows: {
				good: {
					async handler(ctx) {
						FLOWS.push("HANDLER -> " + ctx.wf.jobId + " -> " + ctx.params.name);
						return "OK -> " + ctx.params.name;
					}
				}
			}
		});

		await broker.start();
		await worker.start();
		await cleanup();
	});

	beforeEach(() => {
		FLOWS = [];
	});

	afterAll(async () => {
		await worker.wf.adapter?.dumpWorkflows("./tmp");
		await cleanup();
		await broker.stop();
		await worker.stop();
	});

	it("should wrap local workflow handler", async () => {
		const job = await broker.wf.run("mw.good", { name: "John" });

		const result = await job.promise();
		expect(result).toBe("OK -> John");

		expect(FLOWS).toEqual([
			"BEFORE -> " + job.id + " -> John",
			"HANDLER -> " + job.id + " -> John",
			"AFTER -> " + job.id + " -> OK -> John"
		]);
	});
});
