const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Common Test", () => {
	let broker;
	let FLOWS = [];

	const cleanup = async () => {
		await broker.wf.cleanUp("test.silent");
		await broker.wf.cleanUp("test.simple");
		await broker.wf.cleanUp("test.error");
		await broker.wf.cleanUp("test.context");
		await broker.wf.cleanUp("test.state");
		await broker.wf.cleanUp("test.signal");
		await broker.wf.cleanUp("test.serial");
		await broker.wf.cleanUp("test.long");
		await broker.wf.cleanUp("test.valid");
		await broker.wf.cleanUp("test.disabled");

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
					async handler() {
						throw new Error("Workflow execution failed");
					}
				},
				context: {
					async handler(ctx) {
						expect(ctx.wf).toStrictEqual({
							name: "test.context",
							jobId: expect.any(String),
							retries: undefined,
							retryAttempts: undefined,
							timeout: undefined,
							setState: expect.any(Function),
							sleep: expect.any(Function),
							task: expect.any(Function),
							waitForSignal: expect.any(Function)
						});
						return true;
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
						await delay(100);
						FLOWS.push("STOP-" + ctx.params.id);
						return `Processed ${ctx.params.id}`;
					}
				},
				long: {
					concurrency: 5,
					async handler(ctx) {
						FLOWS.push("START-" + ctx.params.id);
						await delay(1000);
						FLOWS.push("STOP-" + ctx.params.id);
						return `Processed ${ctx.params.id}`;
					}
				},
				valid: {
					params: {
						name: { type: "string" }
					},
					async handler(ctx) {
						return `Hello, ${ctx.params.name}`;
					}
				},
				disabled: {
					enabled: false,
					async handler() {
						return "This workflow is disabled";
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
		await (
			await broker.wf.getAdapter()
		).dumpWorkflows("./tmp", [
			"test.silent",
			"test.simple",
			"test.error",
			"test.context",
			"test.state",
			"test.signal",
			"test.serial",
			"test.long",
			"test.valid",
			"test.disabled"
		]);
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
			nodeID: broker.nodeID,
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
			createdAt: expect.epoch(),
			payload: { name: "World" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, World");

		const job2 = await broker.wf.get("test.simple", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "World" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: "Hello, World"
		});

		const events = await broker.wf.getEvents("test.simple", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
		]);
	});

	it("should execute a simple workflow with specified jobId and return the expected result", async () => {
		const job = await broker.wf.run("test.simple", { name: "World" }, { jobId: "myJobId" });
		expect(job).toStrictEqual({
			id: "myJobId",
			createdAt: expect.epoch(),
			payload: { name: "World" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("Hello, World");

		const job2 = await broker.wf.get("test.simple", job.id);
		expect(job2).toStrictEqual({
			id: "myJobId",
			createdAt: expect.epoch(),
			payload: { name: "World" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: "Hello, World"
		});
	});

	it("should return null if the job is removed", async () => {
		const job = await broker.wf.run("test.simple", { name: "ephemeral" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "ephemeral" },
			promise: expect.any(Function)
		});

		await broker.wf.remove("test.simple", job.id);

		const job2 = await broker.wf.get("test.simple", job.id);
		expect(job2).toBeNull();
	});

	it("should handle workflow errors correctly", async () => {
		const job = await broker.wf.run("test.error", { name: "Error" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "Error" },
			promise: expect.any(Function)
		});

		await expect(job.promise()).rejects.toThrow("Workflow execution failed");

		const job2 = await broker.wf.get("test.error", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "Error" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 200),
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

		// Expects in the action handler
		await job.promise();
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
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				state: "IN_PROGRESS",
				taskId: 1,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
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
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				state: { progress: 50, msg: "IN_PROGRESS" },
				taskId: 1,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
		]);
	});

	it("should handle workflow signals correctly", async () => {
		await broker.wf.removeSignal("signal.first", 12345);

		const job = await broker.wf.run("test.signal", { a: 5 });

		await delay(500);

		const stateBefore = await broker.wf.getState("test.signal", job.id);
		expect(stateBefore).toBe("beforeSignal");

		await broker.wf.triggerSignal("signal.first", 12345, { user: "John" });

		await delay(500);

		const stateAfter = await broker.wf.getState("test.signal", job.id);
		expect(stateAfter).toBe("afterSignal");

		const result = await job.promise();
		expect(result).toStrictEqual({ result: "OK", signalData: { user: "John" } });

		const events = await broker.wf.getEvents("test.signal", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				state: "beforeSignal",
				taskId: 1,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: broker.nodeID,
				signalKey: 12345,
				signalName: "signal.first",
				taskId: 2,
				taskType: "signal-wait",
				ts: expect.epoch(),
				type: "task"
			},
			{
				duration: expect.withinRange(400, 600),
				nodeID: broker.nodeID,
				result: { user: "John" },
				signalKey: 12345,
				signalName: "signal.first",
				taskId: 3,
				taskType: "signal-end",
				ts: expect.epoch(),
				type: "task"
			},
			{
				nodeID: broker.nodeID,
				state: "afterSignal",
				taskId: 4,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
		]);

		const job2 = await broker.wf.get("test.signal", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { a: 5 },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
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

			// console.log(FLOWS);
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

	it("should not execute disabled workflow job", async () => {
		const job = await broker.wf.run("test.disabled");

		await delay(1000);

		const job2 = await broker.wf.get("test.disabled", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch()
		});

		const events = await broker.wf.getEvents("test.disabled", job.id);
		expect(events.length).toBe(0);
	});

	describe("Workflow parameter validation", () => {
		it("should validate workflow parameters", async () => {
			const job = await broker.wf.run("test.valid", { name: "John" });
			expect(job).toStrictEqual({
				id: expect.any(String),
				createdAt: expect.epoch(),
				payload: { name: "John" },
				promise: expect.any(Function)
			});

			const result = await job.promise();
			expect(result).toBe("Hello, John");

			const job2 = await broker.wf.get("test.valid", job.id);
			expect(job2).toStrictEqual({
				id: expect.any(String),
				createdAt: expect.epoch(),
				payload: { name: "John" },
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				nodeID: broker.nodeID,
				duration: expect.withinRange(0, 100),
				success: true,
				result: "Hello, John"
			});
		});

		it("should failed the job with validation error", async () => {
			const job = await broker.wf.run("test.valid", { name: 123 });
			expect(job).toStrictEqual({
				id: expect.any(String),
				createdAt: expect.epoch(),
				payload: { name: 123 },
				promise: expect.any(Function)
			});

			await expect(job.promise()).rejects.toThrow("Parameters validation error!");

			const job2 = await broker.wf.get("test.valid", job.id);
			expect(job2).toStrictEqual({
				id: expect.any(String),
				createdAt: expect.epoch(),
				payload: { name: 123 },
				startedAt: expect.epoch(),
				finishedAt: expect.epoch(),
				nodeID: broker.nodeID,
				duration: expect.withinRange(0, 100),
				success: false,
				error: {
					name: "ValidationError",
					message: "Parameters validation error!",
					code: 422,
					type: "VALIDATION_ERROR",
					nodeID: broker.nodeID,
					data: [
						{
							type: "string",
							actual: 123,
							field: "name",
							message: "The 'name' field must be a string."
						}
					],
					stack: expect.any(String),
					retryable: false
				}
			});
		});
	});
});

describe("Workflows Remote worker Test", () => {
	let broker, worker;

	const cleanup = async () => {
		await broker.wf.cleanUp("remote.good");
		await broker.wf.cleanUp("remote.bad");
		await broker.wf.cleanUp("remote.signal");
		await broker.wf.cleanUp("remote.new");

		await broker.wf.removeSignal("signal.remote", 9999);
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			nodeID: "broker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		worker = new ServiceBroker({
			logger: false,
			nodeID: "worker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		worker.createService({
			name: "remote",
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
				},
				signal: {
					async handler(ctx) {
						await ctx.wf.setState("beforeSignal");
						const signalData = await ctx.wf.waitForSignal("signal.remote", 9999);
						await ctx.wf.setState("afterSignal");
						return { result: "OK", signalData };
					}
				}
			}
		});

		await broker.start();
		await worker.start();
		await cleanup();
	});

	afterAll(async () => {
		await worker.wf.adapter?.dumpWorkflows("./tmp", [
			"remote.good",
			"remote.bad",
			"remote.signal",
			"remote.new"
		]);
		await cleanup();
		await broker.stop();
		await worker.stop();
	});

	it("should execute a simple workflow and return the expected result", async () => {
		const job = await broker.wf.run("remote.good", { name: "John" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBe("OK");

		const job2 = await broker.wf.get("remote.good", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: worker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: "OK"
		});

		const events = await broker.wf.getEvents("remote.good", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.epoch(), type: "started" },
			{ nodeID: "worker", ts: expect.epoch(), type: "finished" }
		]);
	});

	it("should execute a simple workflow and return the expected result later", async () => {
		const job = await broker.wf.run("remote.good", { name: "John" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			promise: expect.any(Function)
		});

		await delay(1000);

		// We call the promise after a delay that the job is already finished
		const result = await job.promise();
		expect(result).toBe("OK");

		const job2 = await broker.wf.get("remote.good", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "John" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: worker.nodeID,
			duration: expect.withinRange(0, 100),
			success: true,
			result: "OK"
		});

		const events = await broker.wf.getEvents("remote.good", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.epoch(), type: "started" },
			{ nodeID: "worker", ts: expect.epoch(), type: "finished" }
		]);
	});

	it("should handle workflow errors correctly", async () => {
		const job = await broker.wf.run("remote.bad", { name: "Error" });
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "Error" },
			promise: expect.any(Function)
		});

		await expect(job.promise()).rejects.toThrow("Some error");

		const job2 = await broker.wf.get("remote.bad", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { name: "Error" },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: worker.nodeID,
			duration: expect.withinRange(0, 100),
			success: false,
			error: {
				name: "Error",
				message: "Some error",
				nodeID: "worker",
				stack: expect.any(String)
			}
		});
	});

	it("should handle workflow signals correctly", async () => {
		await broker.wf.removeSignal("signal.remote", 9999);

		const job = await broker.wf.run("remote.signal", { a: 5 });

		await delay(500);

		const stateBefore = await broker.wf.getState("remote.signal", job.id);
		expect(stateBefore).toBe("beforeSignal");

		await broker.wf.triggerSignal("signal.remote", 9999, { user: "John" });

		await delay(500);

		const stateAfter = await broker.wf.getState("remote.signal", job.id);
		expect(stateAfter).toBe("afterSignal");

		const result = await job.promise();
		expect(result).toStrictEqual({ result: "OK", signalData: { user: "John" } });

		const events = await broker.wf.getEvents("remote.signal", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.epoch(), type: "started" },
			{
				nodeID: "worker",
				state: "beforeSignal",
				taskId: 1,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				signalKey: 9999,
				signalName: "signal.remote",
				taskId: 2,
				taskType: "signal-wait",
				ts: expect.epoch(),
				type: "task"
			},
			{
				duration: expect.withinRange(400, 600),
				nodeID: "worker",
				result: { user: "John" },
				signalKey: 9999,
				signalName: "signal.remote",
				taskId: 3,
				taskType: "signal-end",
				ts: expect.epoch(),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "afterSignal",
				taskId: 4,
				taskType: "state",
				ts: expect.epoch(),
				type: "task"
			},
			{ nodeID: "worker", ts: expect.epoch(), type: "finished" }
		]);

		const job2 = await broker.wf.get("remote.signal", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { a: 5 },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: worker.nodeID,
			duration: expect.withinRange(400, 600),
			state: "afterSignal",
			success: true,
			result: { result: "OK", signalData: { user: "John" } }
		});
	});

	describe("List functions", () => {
		it("should list all completed jobIds", async () => {
			const jobs = await broker.wf.listCompletedJobs("remote.good");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(2);
			expect(jobs[0]).toStrictEqual({ id: expect.any(String), finishedAt: expect.epoch() });
		});

		it("should list all failed jobIds", async () => {
			const jobs = await broker.wf.listFailedJobs("remote.bad");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(1);
			expect(jobs[0]).toStrictEqual({ id: expect.any(String), finishedAt: expect.epoch() });
		});

		it("should return empty lists", async () => {
			const jobs = await broker.wf.listActiveJobs("remote.good");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(0);

			const jobs2 = await broker.wf.listWaitingJobs("remote.good");
			expect(jobs2).toBeInstanceOf(Array);
			expect(jobs2.length).toBe(0);

			const jobs3 = await broker.wf.listDelayedJobs("remote.good");
			expect(jobs3).toBeInstanceOf(Array);
			expect(jobs3.length).toBe(0);
		});

		it("should list waiting jobs", async () => {
			const job1 = await broker.wf.run("remote.new", { name: "John" });
			const jobs = await broker.wf.listWaitingJobs("remote.new");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(1);
			expect(jobs[0]).toStrictEqual({ id: job1.id });
		});

		it("should list active jobs", async () => {
			await broker.wf.removeSignal("signal.remote", 8888);

			const job1 = await broker.wf.run("remote.signal", { name: "John" });

			const jobs = await broker.wf.listActiveJobs("remote.signal");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(1);
			expect(jobs[0]).toStrictEqual({ id: job1.id });

			await broker.wf.triggerSignal("signal.remote", 8888);
		});

		it("should list delayed jobs", async () => {
			const job1 = await broker.wf.run("remote.good", { name: "John" }, { delay: "5m" });

			const jobs = await broker.wf.listDelayedJobs("remote.good");
			expect(jobs).toBeInstanceOf(Array);
			expect(jobs.length).toBe(1);
			expect(jobs[0]).toStrictEqual({ id: job1.id, promoteAt: job1.promoteAt });
		});
	});
});
