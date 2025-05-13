const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");

describe("Workflows Common Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("test.silent");
		await broker.wf.cleanup("test.simple");
		await broker.wf.cleanup("test.error");
		await broker.wf.cleanup("test.context");
		await broker.wf.cleanup("test.state");
		await broker.wf.cleanup("test.signal");
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
				long: {
					concurrency: 5,
					async handler(ctx) {
						await new Promise(resolve => setTimeout(resolve, 1000));
						return `Processed ${ctx.params.id}`;
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

	it("should execute a silent workflow with empty params", async () => {
		const job = await broker.wf.run("test.silent");
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			promise: expect.any(Function)
		});

		const result = await job.promise();
		expect(result).toBeUndefined();

		const job2 = await broker.wf.get("test.silent", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.any(Number),
			success: true
		});
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
			duration: expect.any(Number),
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
			duration: expect.any(Number),
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
	});
});
