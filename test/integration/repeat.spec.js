const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup");

describe("Workflows Repeat Test", () => {
	let broker;
	let FLOWS = [];

	const cleanup = async () => {
		await broker.wf.cleanup("repeat.work");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", maintenanceTime: 3 })]
		});

		broker.createService({
			name: "repeat",
			workflows: {
				work: {
					async handler(ctx) {
						FLOWS.push(ctx.wf.jobId);
						return `Worked`;
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

	it("should throw error if no jobID for repeat job", async () => {
		expect(
			broker.wf.run("repeat.work", null, { repeat: { cron: "*/5 * * * * *" } })
		).rejects.toThrow("Job ID is required for repeatable jobs");
	});

	it("should throw error if endDate is older", async () => {
		expect(
			broker.wf.run("repeat.work", null, {
				jobId: "rep1",
				repeat: { cron: "*/5 * * * * *", endDate: Date.now() - 5000 }
			})
		).rejects.toThrow("Repeatable job is expired at");
	});

	it("should repeat the job with cron", async () => {
		const job = await broker.wf.run("repeat.work", null, {
			jobId: "rep1",
			repeat: { cron: "*/5 * * * * *" }
		});

		expect(job).toStrictEqual({
			id: "rep1",
			promise: expect.any(Function),
			repeat: { cron: "*/5 * * * * *" },
			repeatCounter: 0,
			createdAt: expect.epoch()
		});

		await delay(30_000);

		const job2 = await broker.wf.get("repeat.work", "rep1");
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			repeat: { cron: "*/5 * * * * *" },
			repeatCounter: expect.withinRange(5, 8)
		});

		await broker.wf.remove("repeat.work", "rep1");

		const job1Flows = FLOWS.filter(j => j.startsWith("rep1"));
		expect(job1Flows.length).withinRange(5, 8);

		for (const jobId of job1Flows) {
			const j = await broker.wf.get("repeat.work", jobId);
			expect(j).toStrictEqual({
				id: jobId,
				parent: "rep1",
				createdAt: expect.greaterThan(job2.createdAt),
				promoteAt: expect.epoch(),
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				duration: expect.withinRange(0, 500),
				success: true,
				result: "Worked",
				repeatCounter: expect.any(Number)
			});
		}
	}, 40_000);

	it("should repeat the job with cron with limit", async () => {
		const job = await broker.wf.run("repeat.work", null, {
			jobId: "rep2",
			repeat: { cron: "*/5 * * * * *", limit: 3 }
		});

		expect(job).toStrictEqual({
			id: "rep2",
			promise: expect.any(Function),
			repeat: { cron: "*/5 * * * * *", limit: 3 },
			repeatCounter: 0,
			createdAt: expect.epoch()
		});

		await delay(30_000);

		const job2 = await broker.wf.get("repeat.work", "rep2");
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			finishedAt: expect.epoch(),
			repeat: { cron: "*/5 * * * * *", limit: 3 },
			repeatCounter: 3
		});

		const job2Flows = FLOWS.filter(j => j.startsWith("rep2"));
		expect(job2Flows.length).toBe(3);

		for (const jobId of job2Flows) {
			const j = await broker.wf.get("repeat.work", jobId);
			expect(j).toStrictEqual({
				id: jobId,
				parent: "rep2",
				createdAt: expect.greaterThanOrEqual(job2.createdAt),
				promoteAt: expect.epoch(),
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				duration: expect.withinRange(0, 500),
				success: true,
				result: "Worked",
				repeatCounter: expect.any(Number)
			});
		}
	}, 40_000);

	it("should repeat the job with cron with endDate", async () => {
		const endDate = Date.now() + 20_000;
		const job = await broker.wf.run("repeat.work", null, {
			jobId: "rep3",
			repeat: { cron: "*/5 * * * * *", endDate }
		});

		expect(job).toStrictEqual({
			id: "rep3",
			promise: expect.any(Function),
			repeat: { cron: "*/5 * * * * *", endDate },
			repeatCounter: 0,
			createdAt: expect.epoch()
		});

		await delay(30_000);

		const job2 = await broker.wf.get("repeat.work", "rep3");
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			finishedAt: expect.epoch(),
			repeat: { cron: "*/5 * * * * *", endDate },
			repeatCounter: expect.withinRange(4, 6)
		});

		const job3Flows = FLOWS.filter(j => j.startsWith("rep3"));
		expect(job3Flows.length).withinRange(4, 6);

		for (const jobId of job3Flows) {
			const j = await broker.wf.get("repeat.work", jobId);
			expect(j).toStrictEqual({
				id: jobId,
				parent: "rep3",
				createdAt: expect.greaterThanOrEqual(job2.createdAt),
				promoteAt: expect.epoch(),
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				duration: expect.withinRange(0, 500),
				success: true,
				result: "Worked",
				repeatCounter: expect.any(Number)
			});
		}
	}, 40_000);
});
