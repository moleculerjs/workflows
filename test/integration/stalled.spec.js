const { ServiceBroker } = require("moleculer");
const { MoleculerRetryableError } = require("moleculer").Errors;
const WorkflowsMiddleware = require("../../src");
const { delay } = require("../utils");
require("../jest.setup.js");

describe("Workflows Stalled Job Test", () => {
	let broker, workerBroker;
	let errorState = 0;
	let FLOWS = [];

	const cleanup = async () => {
		await workerBroker.wf.cleanup("stalled.fiveSec");
		await workerBroker.wf.cleanup("stalled.tenSec");
		await workerBroker.wf.cleanup("stalled.complex");
		await workerBroker.wf.removeSignal("email.verification", 1);
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			nodeID: "master",
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		workerBroker = new ServiceBroker({
			nodeID: "worker",
			logger: false,

			middlewares: [
				WorkflowsMiddleware({
					adapter: "Redis",
					maintenanceTime: 3,
					lockExpiration: 5
				})
			]
		});

		workerBroker.createService({
			name: "stalled",
			workflows: {
				fiveSec: {
					async handler() {
						for (let i = 0; i < 5; i++) {
							await delay(1000);
						}
						return `It took 5 seconds`;
					}
				},
				tenSec: {
					async handler() {
						for (let i = 0; i < 10; i++) {
							await delay(1000);
						}
						return `It took 10 seconds`;
					}
				},

				complex: {
					name: "complex",
					async handler(ctx) {
						const isValidEmail = await ctx.call("users.checkEmail", {
							email: ctx.params.email
						});

						if (!isValidEmail) {
							throw new Error("Invalid email");
						}

						await ctx.wf.setState("registering");

						const user = await ctx.call("users.register", {
							email: ctx.params.email,
							name: ctx.params.name
						});

						await ctx.wf.setState("registered");

						// Send event to Moleculer services
						await ctx.emit("user.registered", user);

						await ctx.wf.sleep(2000);

						await ctx.call("mail.send", { type: "verification", user });

						await ctx.wf.setState("waiting_verification");

						try {
							await ctx.wf.waitForSignal("email.verification", user.id, {
								timeout: "1 hour"
							});
							await ctx.wf.setState("VERIFIED");
						} catch (err) {
							if (err.name == "WorkflowTaskTimeoutError") {
								// Registraion not verified in 1 hour, remove the user
								await ctx.call("user.remove", { id: user.id });
								return null;
							}

							// Other error is thrown further
							throw err;
						}

						// Set user verified and save
						user.verified = true;
						await ctx.call("users.update", user);

						// Send event to Moleculer services
						await ctx.broadcast("user.verified", user);

						// Other non-moleculer related workflow task
						await ctx.wf.task("httpPost", async () => {
							FLOWS.push("taskCall - httpPost");

							if (errorState == 2) {
								errorState--;
								throw new MoleculerRetryableError("HTTP error");
							}

							await delay(100);
						});

						// Send welcome email
						await ctx.call("mail.send", { type: "welcome", user });

						// Set the workflow state to done (It can be a string, number, or object)
						await ctx.wf.setState("DONE");

						// It will be stored as a result value to the workflow in event history
						return user;
					}
				}
			}
		});

		workerBroker.createService({
			name: "users",
			actions: {
				checkEmail() {
					FLOWS.push("actionCall - users.checkEmail");
					return true;
				},

				register(ctx) {
					FLOWS.push("actionCall - users.register");
					return {
						id: 1,
						email: ctx.params.email,
						name: ctx.params.name
					};
				},

				update() {
					FLOWS.push("actionCall - users.update");
					return true;
				},

				remove() {
					FLOWS.push("actionCall - users.remove");
					return true;
				}
			},

			events: {
				"user.registered"() {
					FLOWS.push("eventCall - user.registered");
				},
				"user.verified"() {
					FLOWS.push("broadcastCall - user.verified");
				}
			}
		});

		workerBroker.createService({
			name: "mail",
			actions: {
				send() {
					FLOWS.push("actionCall - mail.send");
					if (errorState == 1) {
						errorState--;
						throw new MoleculerRetryableError("SMTP error");
					}
					return true;
				}
			}
		});

		await broker.start();
		await workerBroker.start();
		await cleanup();
	});

	afterAll(async () => {
		// await cleanup();
		await broker.stop();
		await workerBroker.stop();
	});

	it("should execute the one minute job without stalling (lock extended)", async () => {
		const job = await broker.wf.run("stalled.tenSec");
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		const result = await job.promise();
		expect(result).toBe(`It took 10 seconds`);

		const events = await broker.wf.getEvents("stalled.tenSec", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.any(Number), type: "started" },
			{ nodeID: "worker", ts: expect.any(Number), type: "finished" }
		]);
	}, 15000);

	it("should execute the one minute job with stalling", async () => {
		const job = await broker.wf.run("stalled.fiveSec");
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		// Wait for job to be started
		await delay(1000);

		await workerBroker.stop();

		// Wait for lock expiration
		await delay(7_000);

		await workerBroker.start();

		const result = await job.promise();
		expect(result).toBe(`It took 5 seconds`);

		const job2 = await broker.wf.get("stalled.fiveSec", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			createdAt: expect.any(Number),
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(15_000, 25_000),
			success: true,
			stalledCounter: 1,
			result: `It took 5 seconds`
		});

		const events = await broker.wf.getEvents("stalled.fiveSec", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.any(Number), type: "started" },
			{ nodeID: "worker", ts: expect.any(Number), type: "stalled" },
			{ nodeID: "worker", ts: expect.any(Number), type: "started" },
			{ nodeID: "worker", ts: expect.any(Number), type: "finished" }
		]);
	}, 25000);

	it("should execute the complex flow with retry (skip tasks at replaying)", async () => {
		await workerBroker.wf.removeSignal("email.verification", 1);
		FLOWS = [];

		const job = await broker.wf.run("stalled.complex", {
			email: "john.doe@example.com",
			name: "John Doe"
		});
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		// Wait for job to be started
		await delay(1000);

		let state = await broker.wf.getState("stalled.complex", job.id);
		expect(state).toBe("registered");

		await delay(2000);

		state = await broker.wf.getState("stalled.complex", job.id);
		expect(state).toBe("waiting_verification");

		await broker.wf.triggerSignal("email.verification", 1);

		const result = await job.promise();
		expect(result).toStrictEqual({
			id: 1,
			email: "john.doe@example.com",
			name: "John Doe",
			verified: true
		});

		const job2 = await broker.wf.get("stalled.complex", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			payload: {
				email: "john.doe@example.com",
				name: "John Doe"
			},
			createdAt: expect.any(Number),
			startedAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(2500, 4500),
			state: "DONE",
			success: true,
			result
		});

		expect(FLOWS).toEqual([
			"actionCall - users.checkEmail",
			"actionCall - users.register",
			"eventCall - user.registered",
			"actionCall - mail.send",
			"actionCall - users.update",
			"broadcastCall - user.verified",
			"taskCall - httpPost",
			"actionCall - mail.send"
		]);

		const events = await broker.wf.getEvents("stalled.complex", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.any(Number), type: "started" },
			{
				action: "users.checkEmail",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 1,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "registering",
				taskId: 2,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "users.register",
				duration: expect.any(Number),
				nodeID: "worker",
				result: { email: "john.doe@example.com", id: 1, name: "John Doe" },
				taskId: 3,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "registered",
				taskId: 4,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				event: "user.registered",
				nodeID: "worker",
				result: [null],
				taskId: 5,
				taskType: "eventEmit",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				taskId: 6,
				taskType: "sleep",
				time: 2000,
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "mail.send",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 7,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "waiting_verification",
				taskId: 8,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				signalKey: 1,
				signalName: "email.verification",
				taskId: 9,
				taskType: "signal",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "VERIFIED",
				taskId: 10,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "users.update",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 11,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				event: "user.verified",
				nodeID: "worker",
				result: [[null]],
				taskId: 12,
				taskType: "actionBroadcast",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				run: "httpPost",
				taskId: 13,
				taskType: "custom",
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "mail.send",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 14,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "DONE",
				taskId: 15,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				ts: expect.any(Number),
				type: "finished"
			}
		]);
	}, 5000);

	it("should execute the complex flow with 2 retries", async () => {
		errorState = 2;
		await workerBroker.wf.removeSignal("email.verification", 1);
		FLOWS = [];

		const job = await broker.wf.run(
			"stalled.complex",
			{
				email: "john.doe@example.com",
				name: "John Doe"
			},
			{ retries: 2 }
		);
		expect(job).toMatchObject({
			id: expect.any(String)
		});

		// Wait for job to be started
		await delay(1000);

		let state = await broker.wf.getState("stalled.complex", job.id);
		expect(state).toBe("registered");

		await delay(2000);

		state = await broker.wf.getState("stalled.complex", job.id);
		expect(state).toBe("waiting_verification");

		await broker.wf.triggerSignal("email.verification", 1);

		const result = await job.promise();
		expect(result).toStrictEqual({
			id: 1,
			email: "john.doe@example.com",
			name: "John Doe",
			verified: true
		});

		const job2 = await broker.wf.get("stalled.complex", job.id);
		expect(job2).toStrictEqual({
			id: expect.any(String),
			payload: {
				email: "john.doe@example.com",
				name: "John Doe"
			},
			createdAt: expect.any(Number),
			startedAt: expect.any(Number),
			promoteAt: expect.any(Number),
			finishedAt: expect.any(Number),
			duration: expect.withinRange(5000, 10_000),
			state: "DONE",
			retries: 2,
			retryAttempts: 2,
			success: true,
			result
		});

		expect(FLOWS).toEqual([
			"actionCall - users.checkEmail",
			"actionCall - users.register",
			"eventCall - user.registered",
			"actionCall - mail.send",
			"actionCall - users.update",
			"broadcastCall - user.verified",
			"taskCall - httpPost",
			"taskCall - httpPost", // Retried
			"actionCall - mail.send",
			"actionCall - mail.send" // Retried
		]);

		const events = await broker.wf.getEvents("stalled.complex", job.id);
		expect(events).toStrictEqual([
			{ nodeID: "worker", ts: expect.any(Number), type: "started" },
			{
				action: "users.checkEmail",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 1,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "registering",
				taskId: 2,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "users.register",
				duration: expect.any(Number),
				nodeID: "worker",
				result: { email: "john.doe@example.com", id: 1, name: "John Doe" },
				taskId: 3,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "registered",
				taskId: 4,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				event: "user.registered",
				nodeID: "worker",
				result: [null],
				taskId: 5,
				taskType: "eventEmit",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				taskId: 6,
				taskType: "sleep",
				time: 2000,
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "mail.send",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 7,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "waiting_verification",
				taskId: 8,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				signalKey: 1,
				signalName: "email.verification",
				taskId: 9,
				taskType: "signal",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "VERIFIED",
				taskId: 10,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				action: "users.update",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 11,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				event: "user.verified",
				nodeID: "worker",
				result: [[null]],
				taskId: 12,
				taskType: "actionBroadcast",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				run: "httpPost",
				taskId: 13,
				taskType: "custom",
				ts: expect.any(Number),
				type: "task",
				error: {
					code: 500,
					message: "HTTP error",
					name: "MoleculerRetryableError",
					retryable: true,
					stack: expect.any(String),
					nodeID: "worker"
				}
			},
			{
				type: "failed",
				ts: expect.any(Number),
				error: {
					code: 500,
					message: "HTTP error",
					name: "MoleculerRetryableError",
					retryable: true,
					stack: expect.any(String),
					nodeID: "worker"
				},
				nodeID: "worker"
			},
			{
				type: "started",
				ts: expect.any(Number),
				nodeID: "worker"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				run: "httpPost",
				taskId: 13,
				taskType: "custom",
				ts: expect.any(Number),
				type: "task"
			},
			{
				duration: expect.any(Number),
				nodeID: "worker",
				action: "mail.send",
				taskId: 14,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task",
				error: {
					code: 500,
					message: "SMTP error",
					name: "MoleculerRetryableError",
					retryable: true,
					stack: expect.any(String),
					nodeID: "worker"
				}
			},
			{
				type: "failed",
				ts: expect.any(Number),
				error: {
					code: 500,
					message: "SMTP error",
					name: "MoleculerRetryableError",
					retryable: true,
					stack: expect.any(String),
					nodeID: "worker"
				},
				nodeID: "worker"
			},
			{
				type: "started",
				ts: expect.any(Number),
				nodeID: "worker"
			},
			{
				action: "mail.send",
				duration: expect.any(Number),
				nodeID: "worker",
				result: true,
				taskId: 14,
				taskType: "actionCall",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				state: "DONE",
				taskId: 15,
				taskType: "state",
				ts: expect.any(Number),
				type: "task"
			},
			{
				nodeID: "worker",
				ts: expect.any(Number),
				type: "finished"
			}
		]);
	}, 10000);
});
