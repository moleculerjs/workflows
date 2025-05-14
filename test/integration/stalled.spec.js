const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Stalled Job Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("stalled.fiveSec");
		await broker.wf.cleanup("stalled.tenSec");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,

			middlewares: [
				WorkflowsMiddleware({
					adapter: "Redis",
					maintenanceTime: 3,
					lockExpiration: 5
				})
			]
		});

		broker.createService({
			name: "stalled",
			workflows: {
				fiveSec: {
					async handler(ctx) {
						for (let i = 0; i < 5; i++) {
							await delay(1000);
						}
						return `It took 5 seconds`;
					}
				},
				tenSec: {
					async handler(ctx) {
						for (let i = 0; i < 10; i++) {
							await delay(1000);
						}
						return `It took 10 seconds`;
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

	it("should execute the one minute job without stalling (lock extended)", async () => {
		const job = await broker.wf.run("stalled.tenSec");
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		const result = await job.promise();
		expect(result).toBe(`It took 10 seconds`);

		const events = await broker.wf.getEvents("stalled.tenSec", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);
	}, 15000);

	it("should execute the one minute job with stalling", async () => {
		const job = await broker.wf.run("stalled.fiveSec");
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		await delay(1000);

		await broker.stop();

		// Wait for lock expiration and stalled maintenance time
		await delay(7_000);

		await broker.start();

		// Wait for stalled putting back (2 rounds) & job execution
		await delay(15_000);

		const job2 = await broker.wf.get("stalled.fiveSec", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(4500, 5500),
			success: true,
			stalledCounter: 1,
			result: `It took 5 seconds`
		});

		const events = await broker.wf.getEvents("stalled.fiveSec", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "stalled" },
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);
	}, 30000);
});
