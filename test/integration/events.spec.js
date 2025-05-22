const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils.js");
require("../jest.setup.js");

let EVENTS = [];

const svc = {
	name: "events",
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
};

describe("Workflows Events Test with 'emit'", () => {
	let broker, worker;

	const cleanup = async () => {
		await broker.wf.cleanUp("events.good");
		await broker.wf.cleanUp("events.bad");
	};

	beforeEach(() => {
		EVENTS = [];
	});

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			nodeID: "broker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", jobEventType: "emit" })]
		});

		broker.createService({
			name: "eventHandler",
			events: {
				"job.**"(ctx) {
					EVENTS.push([ctx.eventName, ctx.eventType, ctx.nodeID, ctx.params]);
				}
			}
		});

		worker = new ServiceBroker({
			logger: false,
			nodeID: "worker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", jobEventType: "emit" })]
		});

		worker.createService(svc);

		await broker.start();
		await worker.start();
		await cleanup();
	});

	afterAll(async () => {
		await worker.wf.adapter?.dumpWorkflows("./tmp", ["events.good", "events.bad"]);
		await cleanup();
		await broker.stop();
		await worker.stop();
	});

	it("should send job events from completed job", async () => {
		const job = await broker.wf.run("events.good", { name: "John" });
		expect(job.id).toEqual(expect.any(String));
		const result = await job.promise();
		expect(result).toBe("OK");

		expect(EVENTS).toEqual([
			[
				"job.events.good.created",
				"emit",
				"broker",
				{
					job: job.id,
					type: "created",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.started",
				"emit",
				"worker",
				{
					job: job.id,
					type: "started",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.finished",
				"emit",
				"worker",
				{
					job: job.id,
					type: "finished",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.completed",
				"emit",
				"worker",
				{
					job: job.id,
					type: "completed",
					workflow: "events.good"
				}
			]
		]);
	});

	it("should send job events from failed job", async () => {
		const job = await broker.wf.run("events.bad", { name: "John" });
		expect(job.id).toEqual(expect.any(String));
		await expect(job.promise()).rejects.toThrow("Some error");

		expect(EVENTS).toEqual([
			[
				"job.events.bad.created",
				"emit",
				"broker",
				{
					job: job.id,
					type: "created",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.started",
				"emit",
				"worker",
				{
					job: job.id,
					type: "started",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.finished",
				"emit",
				"worker",
				{
					job: job.id,
					type: "finished",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.failed",
				"emit",
				"worker",
				{
					job: job.id,
					type: "failed",
					workflow: "events.bad"
				}
			]
		]);
	});
});

describe("Workflows Events Test with 'broadcast'", () => {
	let broker, worker;

	const cleanup = async () => {
		await broker.wf.cleanUp("events.good");
		await broker.wf.cleanUp("events.bad");
	};

	beforeEach(() => {
		EVENTS = [];
	});

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			nodeID: "broker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", jobEventType: "broadcast" })]
		});

		broker.createService({
			name: "eventHandler",
			events: {
				"job.**"(ctx) {
					EVENTS.push([ctx.eventName, ctx.eventType, ctx.nodeID, ctx.params]);
				}
			}
		});

		worker = new ServiceBroker({
			logger: false,
			nodeID: "worker",
			transporter: "Redis",
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", jobEventType: "broadcast" })]
		});

		worker.createService(svc);

		await broker.start();
		await worker.start();
		await cleanup();
	});

	afterAll(async () => {
		await worker.wf.adapter?.dumpWorkflows("./tmp", ["events.good", "events.bad"]);
		await cleanup();
		await broker.stop();
		await worker.stop();
	});

	it("should send job events from completed job", async () => {
		const job = await broker.wf.run("events.good", { name: "John" });
		expect(job.id).toEqual(expect.any(String));
		const result = await job.promise();
		expect(result).toBe("OK");

		expect(EVENTS).toEqual([
			[
				"job.events.good.created",
				"broadcastLocal",
				"broker",
				{
					job: job.id,
					type: "created",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.started",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "started",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.finished",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "finished",
					workflow: "events.good"
				}
			],
			[
				"job.events.good.completed",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "completed",
					workflow: "events.good"
				}
			]
		]);
	});

	it("should send job events from failed job", async () => {
		const job = await broker.wf.run("events.bad", { name: "John" });
		expect(job.id).toEqual(expect.any(String));
		await expect(job.promise()).rejects.toThrow("Some error");

		expect(EVENTS).toEqual([
			[
				"job.events.bad.created",
				"broadcastLocal",
				"broker",
				{
					job: job.id,
					type: "created",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.started",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "started",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.finished",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "finished",
					workflow: "events.bad"
				}
			],
			[
				"job.events.bad.failed",
				"broadcast",
				"worker",
				{
					job: job.id,
					type: "failed",
					workflow: "events.bad"
				}
			]
		]);
	});
});
