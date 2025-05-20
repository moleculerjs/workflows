const { ServiceBroker } = require("moleculer");
const { MoleculerError } = require("moleculer").Errors;
const WorkflowsMiddleware = require("../../src");
require("../jest.setup.js");

describe("Workflows Serialization Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("serialize.good");
		await broker.wf.cleanUp("serialize.bad");
		await broker.wf.removeSignal("signal.serialize");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [
				WorkflowsMiddleware({
					adapter: { type: "Redis", options: { serializer: "MsgPack" } }
				})
			]
		});

		broker.createService({
			name: "serialize",
			workflows: {
				good: {
					async handler(ctx) {
						const signalResponse = await ctx.wf.waitForSignal("signal.serialize");
						await ctx.wf.setState({ progresss: 50, status: "running" });
						await ctx.wf.task("myGoodTask", async () => {
							return { do: "something" };
						});
						return { ...ctx.params, result: "OK", signalResponse };
					}
				},
				bad: {
					async handler(ctx) {
						await ctx.wf.task("myBadTask", async () => {
							throw new MoleculerError("Something went wrong", 500, "ERR_BAD", {
								a: 5
							});
						});
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

	it("should execute a normal job", async () => {
		const now = Date.now();
		const job = await broker.wf.run("serialize.good", {
			id: 5,
			name: "John",
			status: true,
			loggedIn: now
		});
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { id: 5, name: "John", status: true, loggedIn: now },
			promise: expect.any(Function)
		});

		await broker.wf.triggerSignal("signal.serialize", null, { abc: 123 });

		const result = await job.promise();
		expect(result).toStrictEqual({
			id: 5,
			name: "John",
			status: true,
			loggedIn: now,
			result: "OK",
			signalResponse: { abc: 123 }
		});

		const state = await broker.wf.getState("serialize.good", job.id);
		expect(state).toStrictEqual({ progresss: 50, status: "running" });

		const job2 = await broker.wf.get("serialize.good", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { id: 5, name: "John", status: true, loggedIn: now },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 50),
			success: true,
			state: { progresss: 50, status: "running" },
			result: {
				id: 5,
				name: "John",
				status: true,
				loggedIn: now,
				result: "OK",
				signalResponse: { abc: 123 }
			}
		});

		const events = await broker.wf.getEvents("serialize.good", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskType: "signal-wait",
				taskId: 1,
				duration: 0,
				signalName: "signal.serialize"
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskType: "signal-end",
				taskId: 2,
				duration: expect.withinRange(0, 50),
				signalName: "signal.serialize",
				result: { abc: 123 }
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskType: "state",
				taskId: 3,
				state: { progresss: 50, status: "running" }
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskType: "custom",
				taskName: "myGoodTask",
				taskId: 4,
				duration: expect.withinRange(0, 50),
				result: { do: "something" }
			},
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "finished" }
		]);
	});

	it("should execute a failed job", async () => {
		const now = Date.now();
		const job = await broker.wf.run("serialize.bad", {
			id: 6,
			name: "Jane",
			status: false,
			loggedIn: now
		});
		expect(job).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { id: 6, name: "Jane", status: false, loggedIn: now },
			promise: expect.any(Function)
		});

		try {
			await job.promise();
			expect(true).toBe(false); // Should not reach here
		} catch (err) {
			expect(err).toBeInstanceOf(MoleculerError);
			expect(err.code).toBe(500);
			expect(err.type).toBe("ERR_BAD");
			expect(err.data).toStrictEqual({ a: 5 });
			expect(err.message).toBe("Something went wrong");
			expect(err.name).toBe("MoleculerError");
		}

		const job2 = await broker.wf.get("serialize.bad", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.epoch(),
			payload: { id: 6, name: "Jane", status: false, loggedIn: now },
			startedAt: expect.epoch(),
			finishedAt: expect.epoch(),
			nodeID: broker.nodeID,
			duration: expect.withinRange(0, 50),
			success: false,
			error: {
				code: 500,
				type: "ERR_BAD",
				data: { a: 5 },
				message: "Something went wrong",
				name: "MoleculerError",
				nodeID: broker.nodeID,
				retryable: false,
				stack: expect.any(String)
			}
		});

		const events = await broker.wf.getEvents("serialize.bad", job.id);
		expect(events).toStrictEqual([
			{ nodeID: broker.nodeID, ts: expect.epoch(), type: "started" },
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "task",
				taskType: "custom",
				taskName: "myBadTask",
				taskId: 1,
				duration: expect.withinRange(0, 50),
				error: {
					code: 500,
					type: "ERR_BAD",
					data: { a: 5 },
					message: "Something went wrong",
					name: "MoleculerError",
					nodeID: broker.nodeID,
					retryable: false,
					stack: expect.any(String)
				}
			},
			{
				nodeID: broker.nodeID,
				ts: expect.epoch(),
				type: "failed",
				error: {
					code: 500,
					type: "ERR_BAD",
					data: { a: 5 },
					message: "Something went wrong",
					name: "MoleculerError",
					nodeID: broker.nodeID,
					retryable: false,
					stack: expect.any(String)
				}
			}
		]);
	});
});
