const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils.js");
require("../jest.setup.js");

describe("Workflows Timeout Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("timeout.simple");
		await broker.wf.removeSignal("signal.timeout", 1);
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			logLevel: "error",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", maintenanceTime: 3 })]
		});

		broker.createService({
			name: "timeout",
			workflows: {
				simple: {
					concurrency: 5,
					timeout: "30s",
					async handler(ctx) {
						await ctx.wf.waitForSignal("signal.timeout", 1);

						return `OK`;
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		await broker.wf.adapter?.dumpWorkflows("./tmp", ["timeout.simple"]);

		// Just for a graceful shutdown
		await broker.wf.triggerSignal("signal.timeout", 1);
		await delay(1000);

		await cleanup();
		await broker.stop();
	});

	it("should failed the timeouted jobs", async () => {
		await broker.wf.removeSignal("signal.timeout", 1);

		const job1 = await broker.wf.run("timeout.simple", null, {});

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			promise: expect.any(Function)
		});

		const job2 = await broker.wf.run("timeout.simple", null, { timeout: "10s" });

		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			timeout: 10_000,
			promise: expect.any(Function)
		});

		await delay(15_000);

		let job1Result = await broker.wf.get("timeout.simple", job1.id);
		expect(job1Result).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch()
		});

		const job2Result = await broker.wf.get("timeout.simple", job2.id);
		expect(job2Result).toStrictEqual({
			id: job2.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(8_000, 15_000),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			success: false,
			timeout: 10_000,
			error: expect.objectContaining({
				name: "WorkflowTimeoutError",
				message: expect.stringMatching(/Job timed out/)
			})
		});

		await delay(20_000);

		job1Result = await broker.wf.get("timeout.simple", job1.id);
		expect(job1Result).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(25_000, 35_000),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			success: false,
			error: expect.objectContaining({
				name: "WorkflowTimeoutError",
				message: expect.stringMatching(/Job timed out/)
			})
		});
	}, 60_000);

	it("should failed the timeouted jobs with retry", async () => {
		await broker.wf.removeSignal("signal.timeout", 1);

		const job1 = await broker.wf.run("timeout.simple", null, { timeout: "10s", retries: 2 });

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			retries: 2,
			retryAttempts: 0,
			timeout: 10_000,
			promise: expect.any(Function)
		});

		await delay(40_000);

		const job2 = await broker.wf.get("timeout.simple", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(15_000, 45_000),
			promoteAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			retries: 2,
			retryAttempts: 2,
			timeout: 10_000,
			success: false,
			error: expect.objectContaining({
				name: "WorkflowTimeoutError",
				message: expect.stringMatching(/Job timed out/)
			})
		});
	}, 60_000);
});
