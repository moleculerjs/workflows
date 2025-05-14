const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Common Test", () => {
	let broker;
	let FLOWS = [];

	const cleanup = async () => {
		await broker.wf.cleanup("test.silent");
		await broker.wf.cleanup("test.simple");
		await broker.wf.cleanup("test.error");
		await broker.wf.cleanup("test.context");
		await broker.wf.cleanup("test.state");
		await broker.wf.cleanup("test.signal");
		await broker.wf.cleanup("test.serial");
		await broker.wf.cleanup("test.long");

		await broker.wf.removeSignal("signal.first", 12345);
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "test",
			workflows: {
				silent: {
					async handler() {
						return;
					}
				},
				simple: {
					async handler(ctx) {
						return `Hello, ${ctx.params?.name}`;
					}
				},
				error: {
					async handler(ctx) {
						throw new Error("Workflow execution failed");
					}
				},
				context: {
					async handler(ctx) {
						return ctx.wf;
					}
				},
				state: {
					async handler(ctx) {
						await ctx.wf.setState(ctx.params.state);
					}
				},
				signal: {
					async handler(ctx) {
						await ctx.wf.setState("beforeSignal");
						const signalData = await ctx.wf.waitForSignal("signal.first", 12345);
						await ctx.wf.setState("afterSignal");
						return { result: "OK", signalData };
					}
				},
				serial: {
					async handler(ctx) {
						FLOWS.push("START-" + ctx.params.id);
						await new Promise(resolve => setTimeout(resolve, 100));
						FLOWS.push("STOP-" + ctx.params.id);
						return `Processed ${ctx.params.id}`;
					}
				},
				long: {
					concurrency: 5,
					async handler(ctx) {
						FLOWS.push("START-" + ctx.params.id);
						await new Promise(resolve => setTimeout(resolve, 1000));
						FLOWS.push("STOP-" + ctx.params.id);
						return `Processed ${ctx.params.id}`;
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	});

	beforeEach(() => {
		FLOWS = [];
	});

	afterAll(async () => {
		await cleanup();
		await broker.stop();
	});

	it("should execute a silent workflow with empty params", async () => {
		const now = Date.now();
		const job = await broker.wf.run("test.silent");
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.greaterThanOrEqual(now),
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBeUndefined();

		const job2 = await broker.wf.get("test.silent", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.greaterThanOrEqual(now),
			startedAt: expect.greaterThanOrEqual(now),
			finishedAt: expect.greaterThanOrEqual(now),
			duration: expect.withinRange(0, 100),
			success: true
		});
		expect(job2.startedAt).toBeGreaterThanOrEqual(job2.createdAt);
		expect(job2.finishedAt).toBeGreaterThanOrEqual(job2.startedAt);
	});

	it("should execute a simple workflow and return the expected result", async () => {
		const job = await broker.wf.run("test.simple", { name: "World" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			payload: { name: "World" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, World");

		const job2 = await broker.wf.get("test.simple", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			payload: { name: "World" },
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(0, 100),
			success: true,
			result: "Hello, World"
		});

		const events = await broker.wf.getEvents("test.simple", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);
	});

	it("should execute a simple workflow with specified jobId and return the expected result", async () => {
		const job = await broker.wf.run("test.simple", { name: "World" }, { jobId: "myJobId" });
		expect(job).toStrictEqual({
			id: "myJobId",
			createdAt: expect.any(Number),
			payload: { name: "World" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, World");

		const job2 = await broker.wf.get("test.simple", job.id);
		expect(job2).toStrictEqual({
			id: "myJobId",
			createdAt: expect.any(Number),
			payload: { name: "World" },
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(0, 100),
			success: true,
			result: "Hello, World"
		});
	});

	it("should handle workflow errors correctly", async () => {
		const job = await broker.wf.run("test.error", { name: "Error" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			payload: { name: "Error" },
			promise: expect.any(Function)
		});

		await expect(job.promise()).rejects.toThrow("Workflow execution failed");

		const job2 = await broker.wf.get("test.error", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			payload: { name: "Error" },
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(0, 100),
			success: false,
			error: {
				name: "Error",
				message: "Workflow execution failed",
				nodeID: broker.nodeID,
				stack: expect.any(String)
			}
		});
	});

	it("should execute a workflow and check ctx.wf properties", async () => {
		const job = await broker.wf.run("test.context", { a: 5 });

		const result = await job.promise();
		expect(result).toStrictEqual({
			name: "test.context",
			jobId: job.id,
			setState: expect.any(Function),
			sleep: expect.any(Function),
			task: expect.any(Function),
			waitForSignal: expect.any(Function)
		});
	});

	it("should set workflow status correctly (string)", async () => {
		const job = await broker.wf.run("test.state", { state: "IN_PROGRESS" });

		await job.promise();

		const state = await broker.wf.getState("test.state", job.id);
		expect(state).toBe("IN_PROGRESS");

		const job2 = await broker.wf.get("test.state", job.id);
		expect(job2.state).toBe("IN_PROGRESS");

		const events = await broker.wf.getEvents("test.state", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{
				nodeID: broker.nodeID,
				state: "IN_PROGRESS",
				taskId: 1,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);
	});

	it("should set workflow status correctly (object)", async () => {
		const job = await broker.wf.run("test.state", {
			state: { progress: 50, msg: "IN_PROGRESS" }
		});

		await job.promise();

		const state = await broker.wf.getState("test.state", job.id);
		expect(state).toStrictEqual({ progress: 50, msg: "IN_PROGRESS" });

		const job2 = await broker.wf.get("test.state", job.id);
		expect(job2.state).toStrictEqual({ progress: 50, msg: "IN_PROGRESS" });

		const events = await broker.wf.getEvents("test.state", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{
				nodeID: broker.nodeID,
				state: { progress: 50, msg: "IN_PROGRESS" },
				taskId: 1,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);
	});

	it("should handle workflow signals correctly", async () => {
		await broker.wf.removeSignal("signal.first", 12345);

		const job = await broker.wf.run("test.signal", { a: 5 });

		await broker.Promise.delay(500);

		const stateBefore = await broker.wf.getState("test.signal", job.id);
		expect(stateBefore).toBe("beforeSignal");

		await broker.wf.triggerSignal("signal.first", 12345, { user: "John" });

		await broker.Promise.delay(500);

		const stateAfter = await broker.wf.getState("test.signal", job.id);
		expect(stateAfter).toBe("afterSignal");

		const result = await job.promise();
		expect(result).toStrictEqual({ result: "OK", signalData: { user: "John" } });

		const events = await broker.wf.getEvents("test.signal", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "started" },
			{
				nodeID: broker.nodeID,
				state: "beforeSignal",
				taskId: 1,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.withinRange(400, 600),
				nodeID: broker.nodeID,
				result: { user: "John" },
				signalKey: 12345,
				signalName: "signal.first",
				taskId: 2,
				taskType: "signal",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: broker.nodeID,
				state: "afterSignal",
				taskId: 3,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.any(Number), type: "finished" }
		]);

		const job2 = await broker.wf.get("test.signal", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			payload: { a: 5 },
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(400, 600),
			state: "afterSignal",
			success: true,
			result: { result: "OK", signalData: { user: "John" } }
		});
	});

	describe("Workflow concurrency", () => {
		it("should execute jobs one-by-one", async () => {
			const promises = [];
			for (let i = 0; i < 5; i++) {
				promises.push((await broker.wf.run("test.serial", { id: i })).promise());
			}

			const results = await Promise.all(promises);
			expect(results.length).toBe(5);
			results.forEach((result, index) => {
				expect(result).toBe(`Processed ${index}`);
			});
			expect(FLOWS).toEqual([
				"START-0",
				"STOP-0",
				"START-1",
				"STOP-1",
				"START-2",
				"STOP-2",
				"START-3",
				"STOP-3",
				"START-4",
				"STOP-4"
			]);
		});

		it("should handle concurrent workflows", async () => {
			const promises = [];
			for (let i = 0; i < 5; i++) {
				promises.push((await broker.wf.run("test.long", { id: i })).promise());
			}

			const results = await Promise.all(promises);
			expect(results.length).toBe(5);
			results.forEach((result, index) => {
				expect(result).toBe(`Processed ${index}`);
			});

			expect(FLOWS).toEqual([
				"START-0",
				"START-1",
				"START-2",
				"START-3",
				"START-4",
				"STOP-0",
				"STOP-1",
				"STOP-2",
				"STOP-3",
				"STOP-4"
			]);
		});

		it("should handle concurrent workflows", async () => {
			const promises = [];
			for (let i = 0; i < 10; i++) {
				promises.push((await broker.wf.run("test.long", { id: i })).promise());
			}

			const results = await Promise.all(promises);
			expect(results.length).toBe(10);
			results.forEach((result, index) => {
				expect(result).toBe(`Processed ${index}`);
			});

			expect(FLOWS.length).toBe(20);
			expect(FLOWS[0]).toBe("START-0");

			expect(FLOWS).toBeItemAfter("START-1", "START-0");
			expect(FLOWS).toBeItemAfter("START-2", "START-1");
			expect(FLOWS).toBeItemAfter("START-3", "START-2");
			expect(FLOWS).toBeItemAfter("START-4", "START-3");
			expect(FLOWS).toBeItemAfter("START-5", "START-4");
			expect(FLOWS).toBeItemAfter("STOP-0", "START-0");
			expect(FLOWS).toBeItemAfter("STOP-1", "START-1");
			expect(FLOWS).toBeItemAfter("STOP-2", "START-2");
			expect(FLOWS).toBeItemAfter("STOP-3", "START-3");
			expect(FLOWS).toBeItemAfter("STOP-4", "START-4");

			expect(FLOWS).toBeItemAfter("START-5", "STOP-0");
			expect(FLOWS).toBeItemAfter("START-6", "STOP-1");
			expect(FLOWS).toBeItemAfter("START-7", "STOP-2");
			expect(FLOWS).toBeItemAfter("START-8", "STOP-3");
			expect(FLOWS).toBeItemAfter("START-9", "STOP-4");
			expect(FLOWS).toBeItemAfter("STOP-5", "START-5");
			expect(FLOWS).toBeItemAfter("STOP-6", "START-6");
			expect(FLOWS).toBeItemAfter("STOP-7", "START-7");
			expect(FLOWS).toBeItemAfter("STOP-8", "START-8");
			expect(FLOWS).toBeItemAfter("STOP-9", "START-9");
		});
	});
});
