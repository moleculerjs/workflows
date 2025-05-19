const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Signal Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("signal.good");
		await broker.wf.cleanUp("signal.time");
		await broker.wf.cleanUp("signal.bad");
		await broker.wf.removeSignal("signal.test", 555);
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			logLevel: "error",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", maintenanceTime: 3 })]
		});

		broker.createService({
			name: "signal",
			workflows: {
				good: {
					backoffDelay: 1000,
					async handler(ctx) {
						await ctx.wf.waitForSignal("signal.test", 555, { timeout: "10s" });
						return `OK`;
					}
				},
				bad: {
					async handler(ctx) {
						await ctx.wf.waitForSignal("signal.test", 555, { timeout: "10s" });
						throw new Error("Some error");
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		await broker.wf.adapter?.dumpWorkflows("./tmp");
		await cleanup();
		await broker.stop();
	});

	it("should failed if signal timed out", async () => {
		await broker.wf.removeSignal("signal.test", 555);

		const job1 = await broker.wf.run("signal.good");

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			promise: expect.any(Function)
		});

		await expect(job1.promise()).rejects.toThrow(
			expect.objectContaining({
				name: "WorkflowSignalTimeoutError",
				message: expect.stringMatching(/Signal timed out/)
			})
		);

		const job2 = await broker.wf.get("signal.good", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(10_000, 15_000),
			finishedAt: expect.epoch(),
			success: false,
			error: expect.objectContaining({
				name: "WorkflowSignalTimeoutError",
				message: expect.stringMatching(/Signal timed out/)
			})
		});

		const events = await broker.wf.getEvents("signal.good", job1.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 1,
				taskType: "signal-wait",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			}
		]);
	}, 20_000);

	it("should not wait twice if the job rerun and signal is timed out (3x failed)", async () => {
		await broker.wf.removeSignal("signal.test", 555);

		const job1 = await broker.wf.run("signal.good", null, { retries: 2 });

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			retries: 2,
			retryAttempts: 0,
			promise: expect.any(Function)
		});

		await expect(job1.promise()).rejects.toThrow(
			expect.objectContaining({
				name: "WorkflowSignalTimeoutError",
				message: expect.stringMatching(/Signal timed out/)
			})
		);

		const job2 = await broker.wf.get("signal.good", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(12_000, 22_000),
			retries: 2,
			retryAttempts: 2,
			promoteAt: expect.epoch(),
			finishedAt: expect.epoch(),
			success: false,
			error: expect.objectContaining({
				name: "WorkflowSignalTimeoutError",
				message: expect.stringMatching(/Signal timed out/)
			})
		});

		const events = await broker.wf.getEvents("signal.good", job1.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 1,
				taskType: "signal-wait",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			}
		]);
	}, 20_000);

	it("should not wait twice if the job rerun and signal is timed out (first failed, last success)", async () => {
		await broker.wf.removeSignal("signal.test", 555);

		const job1 = await broker.wf.run("signal.good", null, { retries: 2 });

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			retries: 2,
			retryAttempts: 0,
			promise: expect.any(Function)
		});

		// No await, it'run in background
		new Promise(resolve => {
			setTimeout(() => {
				broker.wf.triggerSignal("signal.test", 555);
				resolve();
			}, 11_000);
		});

		const result = await job1.promise();
		expect(result).toBe("OK");

		const job2 = await broker.wf.get("signal.good", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(10_000, 18_000),
			retries: 2,
			retryAttempts: 1,
			promoteAt: expect.epoch(),
			finishedAt: expect.epoch(),
			success: true,
			result: "OK"
		});

		const events = await broker.wf.getEvents("signal.good", job1.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 1,
				taskType: "signal-wait",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: expect.objectContaining({
					name: "WorkflowSignalTimeoutError",
					message: expect.stringMatching(/Signal timed out/)
				})
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "finished"
			}
		]);
	}, 20_000);

	it("should success if signal received in time", async () => {
		await broker.wf.removeSignal("signal.test", 555);

		const job1 = await broker.wf.run("signal.good");

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			promise: expect.any(Function)
		});

		// No await, it'run in background
		new Promise(resolve => {
			setTimeout(() => {
				broker.wf.triggerSignal("signal.test", 555);
				resolve();
			}, 5_000);
		});

		const result = await job1.promise();
		expect(result).toBe("OK");

		const job2 = await broker.wf.get("signal.good", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(4_000, 10_000),
			finishedAt: expect.epoch(),
			success: true,
			result: "OK"
		});

		const events = await broker.wf.getEvents("signal.good", job1.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 1,
				taskType: "signal-wait",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "finished"
			}
		]);
	}, 10_000);

	it("should success if signal already exists", async () => {
		await broker.wf.triggerSignal("signal.test", 555);

		const job1 = await broker.wf.run("signal.good");

		expect(job1).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			promise: expect.any(Function)
		});

		const result = await job1.promise();
		expect(result).toBe("OK");

		const job2 = await broker.wf.get("signal.good", job1.id);
		expect(job2).toStrictEqual({
			id: job1.id,
			createdAt: expect.epoch(),
			startedAt: expect.epoch(),
			duration: expect.withinRange(0, 1000),
			finishedAt: expect.epoch(),
			success: true,
			result: "OK"
		});

		const events = await broker.wf.getEvents("signal.good", job1.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 1,
				taskType: "signal-wait",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskId: 2,
				taskType: "signal-end",
				signalName: "signal.test",
				signalKey: 555,
				duration: expect.any(Number),
				timeout: "10s"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "finished"
			}
		]);
	});
});
