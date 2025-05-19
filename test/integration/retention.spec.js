const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Retention Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("retention.good");
		await broker.wf.cleanUp("retention.bad");
	};

	const createBroker = async wfOpts => {
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
					...wfOpts,
					async handler() {
						return "OK";
					}
				},
				bad: {
					...wfOpts,
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

	describe("Retention time", () => {
		it("should remove completed job after retention time", async () => {
			await createBroker({ retention: "10 sec" });
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

			await delay(20_000);

			const job3 = await broker.wf.get("retention.good", job.id);
			expect(job3).toBeNull();
		}, 25_000);

		it("should remove failed job after retention time", async () => {
			await createBroker({ retention: "10 sec" });
			const job = await broker.wf.run("retention.bad");
			expect(job.id).toEqual(expect.any(String));
			await expect(job.promise()).rejects.toThrow("Some error");

			const job2 = await broker.wf.get("retention.bad", job.id);
			expect(job2).toStrictEqual({
				id: expect.any(String),
				createdAt: expect.epoch(),
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				duration: expect.withinRange(0, 200),
				success: false,
				error: expect.objectContaining({
					message: "Some error",
					name: "Error"
				})
			});

			await delay(20_000);

			const job3 = await broker.wf.get("retention.bad", job.id);
			expect(job3).toBeNull();
		}, 25_000);
	});

	describe("Remove on completed", () => {
		it("should remove completed job after finished", async () => {
			await createBroker({ removeOnCompleted: true });
			const goodJob = await broker.wf.run("retention.good");
			expect(goodJob.id).toEqual(expect.any(String));
			await goodJob.promise();

			const goodJob2 = await broker.wf.get("retention.good", goodJob.id);
			expect(goodJob2).toBeNull();

			const badJob = await broker.wf.run("retention.bad");
			expect(badJob.id).toEqual(expect.any(String));
			await expect(badJob.promise()).rejects.toThrow("Some error");

			const badJob2 = await broker.wf.get("retention.bad", badJob.id);
			expect(badJob2).toBeDefined();
		});
	});

	describe("Remove on failed", () => {
		it("should remove failed job after finished", async () => {
			await createBroker({ removeOnFailed: true });
			const goodJob = await broker.wf.run("retention.good");
			expect(goodJob.id).toEqual(expect.any(String));
			await goodJob.promise();

			const goodJob2 = await broker.wf.get("retention.good", goodJob.id);
			expect(goodJob2).toBeDefined();

			const badJob = await broker.wf.run("retention.bad");
			expect(badJob.id).toEqual(expect.any(String));
			await expect(badJob.promise()).rejects.toThrow("Some error");

			const badJob2 = await broker.wf.get("retention.bad", badJob.id);
			expect(badJob2).toBeNull();
		});
	});
});
