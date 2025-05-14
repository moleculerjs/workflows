const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Stalled Job Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("stalled.fiveSec");
		await broker.wf.cleanup("stalled.tenSec");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			//logger: true,
			logger: {
				type: "Console",
				options: {
					formatter: "short",
					level: {
						WORKFLOWS: "debug",
						"*": "info"
					}
				}
			},
			middlewares: [
				WorkflowsMiddleware({
					adapter: { type: "Redis", options: { lockExpiration: 5000 } },
					maintenanceTime: 3
				})
			]
		});

		broker.createService({
			name: "stalled",
			workflows: {
				fiveSec: {
					async handler(ctx) {
						for (let i = 0; i < 5; i++) {
							await new Promise(resolve => setTimeout(resolve, 1000));
						}
						return `It took 5 seconds`;
					}
				},
				tenSec: {
					async handler(ctx) {
						for (let i = 0; i < 10; i++) {
							await new Promise(resolve => setTimeout(resolve, 1000));
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
		//await cleanup();
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

		await new Promise(resolve => setTimeout(resolve, 1000));

		await broker.stop();

		// Wait for lock expiration and stalled maintenance time
		await new Promise(resolve => setTimeout(resolve, 7 * 1000));

		await broker.start();

		// Wait for stalled putting back (2+1 rounds) & job execution
		await new Promise(resolve => setTimeout(resolve, 15 * 1000));

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
