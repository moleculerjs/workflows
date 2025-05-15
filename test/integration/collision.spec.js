const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Job ID collision Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("collision.good");
		await broker.wf.cleanup("collision.bad");
	};

	const createBroker = async jobIdCollision => {
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
			middlewares: [
				WorkflowsMiddleware({ adapter: "Redis", maintenanceTime: 5, jobIdCollision })
			]
		});

		broker.createService({
			name: "collision",
			workflows: {
				good: {
					async handler() {
						return "OK";
					}
				},
				bad: {
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

	describe("Reject policy", () => {
		it("should enable create jobs with generated UUID", async () => {
			await createBroker();
			const job1 = await broker.wf.run("collision.good");
			expect(job1.id).toBeDefined();

			const job2 = await broker.wf.run("collision.good");
			expect(job2.id).toBeDefined();
		});

		it("should reject the job if jobId collision is detected", async () => {
			await createBroker();
			const job = await broker.wf.run("collision.good", null, { jobId: "j1" });
			expect(job.id).toBe("j1");

			await expect(broker.wf.run("collision.good", null, { jobId: "j1" })).rejects.toThrow(
				"Job ID 'j1' already exists."
			);
		});
	});

	describe("Skip policy", () => {
		it("should return the same job before finished", async () => {
			await createBroker("skip");

			const job1 = await broker.wf.run("collision.good", { a: 5 }, { jobId: "j1" });
			expect(job1.id).toBe("j1");

			const job2 = await broker.wf.run("collision.good", { a: 5 }, { jobId: "j1" });
			expect(job2.id).toBe("j1");

			expect(job1).toStrictEqual({
				id: "j1",
				createdAt: expect.epoch(),
				payload: { a: 5 },
				promise: expect.any(Function)
			});

			expect(JSON.stringify(job1)).toEqual(JSON.stringify(job2));

			await job1.promise();
			await job2.promise();

			const job3 = await broker.wf.get("collision.good", "j1");

			expect(job3).toStrictEqual({
				id: "j1",
				createdAt: expect.epoch(),
				payload: { a: 5 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true
			});
		});

		it("should return the same job after finished", async () => {
			await createBroker("skip");

			const job1 = await broker.wf.run("collision.good", { a: 6 }, { jobId: "j2" });
			expect(job1.id).toBe("j2");

			const res = await job1.promise();
			expect(res).toBe("OK");

			expect(job1).toStrictEqual({
				id: "j2",
				createdAt: expect.epoch(),
				payload: { a: 6 },
				promise: expect.any(Function)
			});

			const job2 = await broker.wf.run("collision.good", { a: 6 }, { jobId: "j2" });
			expect(job2.id).toBe("j2");

			expect(job2).toStrictEqual({
				id: "j2",
				createdAt: expect.epoch(),
				payload: { a: 6 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true,
				promise: expect.any(Function)
			});

			const res2 = await job2.promise();
			expect(res2).toBe("OK");

			const job3 = await broker.wf.get("collision.good", "j2");

			expect(job3).toStrictEqual({
				id: "j2",
				createdAt: expect.epoch(),
				payload: { a: 6 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true
			});

			const events = await broker.wf.getEvents("collision.good", "j2");
			expect(events).toStrictEqual([
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
			]);
		});
	});

	describe("Rerun policy", () => {
		it("should return the same job before finished", async () => {
			await createBroker("rerun");

			const job1 = await broker.wf.run("collision.good", { a: 7 }, { jobId: "j3" });
			expect(job1.id).toBe("j3");

			const job2 = await broker.wf.run("collision.good", { a: 7 }, { jobId: "j3" });
			expect(job2.id).toBe("j3");

			expect(job1).toStrictEqual({
				id: "j3",
				createdAt: expect.epoch(),
				payload: { a: 7 },
				promise: expect.any(Function)
			});

			expect(JSON.stringify(job1)).toEqual(JSON.stringify(job2));

			await job1.promise();
			await job2.promise();

			const job3 = await broker.wf.get("collision.good", "j3");

			expect(job3).toStrictEqual({
				id: "j3",
				createdAt: expect.epoch(),
				payload: { a: 7 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true
			});
		});

		it("should rerun the same job after finished", async () => {
			await createBroker("rerun");

			const job1 = await broker.wf.run("collision.good", { a: 8 }, { jobId: "j4" });
			expect(job1.id).toBe("j4");

			await job1.promise();

			expect(job1).toStrictEqual({
				id: "j4",
				createdAt: expect.epoch(),
				payload: { a: 8 },
				promise: expect.any(Function)
			});

			const job2 = await broker.wf.run("collision.good", { a: 8 }, { jobId: "j4" });
			expect(job2.id).toBe("j4");

			expect(job2).toStrictEqual({
				id: "j4",
				createdAt: expect.epoch(),
				payload: { a: 8 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true,
				promise: expect.any(Function)
			});

			await job2.promise();

			const job3 = await broker.wf.get("collision.good", "j4");

			expect(job3).toStrictEqual({
				id: "j4",
				createdAt: expect.epoch(),
				payload: { a: 8 },
				startedAt: expect.epoch(),
				duration: expect.any(Number),
				finishedAt: expect.epoch(),
				result: "OK",
				success: true
			});

			const events = await broker.wf.getEvents("collision.good", "j4");
			expect(events).toStrictEqual([
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" },
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
				{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
			]);
		});
	});
});
