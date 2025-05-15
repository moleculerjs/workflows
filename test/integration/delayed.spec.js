const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Delayed Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("delayed.simple");
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

	it("should execute a delayed job", async () => {
		const now = Date.now();
		const job = await broker.wf.run("delayed.simple", { name: "John" }, { delay: "15s" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			delay: 15000,
			payload: { name: "John" },
			promoteAt: expect.greaterThanOrEqual(now + 15000),
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe(`Hello, it's called`);

		const job2 = await broker.wf.get("delayed.simple", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			delay: 15000,
			promoteAt: expect.greaterThanOrEqual(now + 15000),
			startedAt: expect.greaterThanOrEqual(job.promoteAt),
			finishedAt: expect.greaterThanOrEqual(job2.startedAt),
			duration: expect.withinRange(0, 10),
			success: true,
			result: `Hello, it's called`
		});
	}, 30000);
});
