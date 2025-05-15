const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Retention Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("retention.good");
		await broker.wf.cleanup("retention.bad");
	};

	const createBroker = async retention => {
		broker = new ServiceBroker({
			logger: false,
			/*logger: {
				type: "Console",
				options: {
					formatter: "short",
					level: {
						WORKFLOWS: "debug",
						"*": "info"
					}
				}
			},*/
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", maintenanceTime: 3 })]
		});

		broker.createService({
			name: "retention",
			workflows: {
				good: {
					retention,
					async handler() {
						return "OK";
					}
				},
				bad: {
					retention,
					async handler() {
						throw new Error("Some error");
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	};

	afterEach(async () => {
		await cleanup();
		await broker.stop();
	});

	it("should remove completed job after retention time", async () => {
		await createBroker("10 sec");
		const job = await broker.wf.run("retention.good");
		expect(job.id).toEqual(expect.any(String));
		await job.promise();

		const job2 = await broker.wf.get("retention.good", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			duration: expect.withinRange(0, 100),
			success: true,
			result: "OK"
		});

		await delay(15_000);

		const job3 = await broker.wf.get("retention.good", job.id);
		expect(job3).toBeNull();
	}, 20_000);

	it("should remove failed job after retention time", async () => {
		await createBroker("10 sec");
		const job = await broker.wf.run("retention.bad");
		expect(job.id).toEqual(expect.any(String));
		await expect(job.promise()).rejects.toThrow("Some error");

		const job2 = await broker.wf.get("retention.bad", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			duration: expect.withinRange(0, 100),
			success: false,
			error: expect.objectContaining({
				message: "Some error",
				name: "Error"
			})
		});

		await delay(15_000);

		const job3 = await broker.wf.get("retention.bad", job.id);
		expect(job3).toBeNull();
	}, 20_000);
});
