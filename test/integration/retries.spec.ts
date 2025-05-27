import { describe, expect, it, beforeAll, afterAll, afterEach } from "vitest";

import { ServiceBroker } from "moleculer";
import { Errors } from "moleculer";
import WorkflowsMiddleware from "../../src/middleware.ts";

import "../vitest-exensions.ts";

describe("Workflows Retries Test", () => {
	describe("Retry in run", () => {
		let broker;
		let FLOWS = [];

		const cleanup = async () => {
			await broker.wf.cleanUp("retry.simple");
			await broker.wf.cleanUp("retry.policy");
		};

		beforeAll(async () => {
			broker = new ServiceBroker({
				logger: false,
				middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
			});

			broker.createService({
				name: "retry",
				workflows: {
					simple: {
						async handler(ctx) {
							FLOWS.push(ctx.wf.name);
							if ((ctx.wf.retryAttempts ?? 0) < 3) {
								throw new Errors.MoleculerRetryableError("Simulated failure");
							}
							return `Success on attempt ${ctx.wf.retryAttempts}`;
						}
					},
					policy: {
						retryPolicy: {
							retries: 3,
							delay: 100,
							maxDelay: 1000,
							factor: 2
						},
						async handler(ctx) {
							FLOWS.push(ctx.wf.name);
							if ((ctx.wf.retryAttempts ?? 0) < 3) {
								throw new Errors.MoleculerRetryableError("Simulated failure");
							}
							return `Success on attempt ${ctx.wf.retryAttempts}`;
						}
					}
				}
			});

			await broker.start();
			await cleanup();
		});

		afterEach(() => {
			FLOWS = [];
		});

		afterAll(async () => {
			await (
				await broker.wf.getAdapter()
			).dumpWorkflows("./tmp", ["retry.simple", "retry.policy"]);
			await cleanup();
			await broker.stop();
		});

		describe("Retry with run retries", () => {
			it("should fail the job without retries", async () => {
				const job = await broker.wf.run("retry.simple");
				expect(job.id).toEqual(expect.any(String));
				await expect(job.promise()).rejects.toThrow("Simulated failure");
				expect(FLOWS).toEqual(["retry.simple"]);
			});

			it("should fail the job after 2 retries", async () => {
				const job = await broker.wf.run("retry.simple", {}, { retries: 2 });
				expect(job.id).toEqual(expect.any(String));
				await expect(job.promise()).rejects.toThrow("Simulated failure");
				expect(FLOWS).toEqual(["retry.simple", "retry.simple", "retry.simple"]);
			});

			it("should success the job after 3 retries", async () => {
				const job = await broker.wf.run("retry.simple", {}, { retries: 3 });
				expect(job.id).toEqual(expect.any(String));
				const result = await job.promise();
				expect(result).toBe("Success on attempt 3");
				expect(FLOWS).toEqual([
					"retry.simple",
					"retry.simple",
					"retry.simple",
					"retry.simple"
				]);
			});
		});

		describe("Retry with workflow policy", () => {
			it("should fail the job without retries", async () => {
				const job = await broker.wf.run("retry.policy", {}, { retries: 0 });
				expect(job.id).toEqual(expect.any(String));
				await expect(job.promise()).rejects.toThrow("Simulated failure");
				expect(FLOWS).toEqual(["retry.policy"]);
			});

			it("should fail the job after 2 retries", async () => {
				const job = await broker.wf.run("retry.policy", {}, { retries: 2 });
				expect(job.id).toEqual(expect.any(String));
				await expect(job.promise()).rejects.toThrow("Simulated failure");
				expect(FLOWS).toEqual(["retry.policy", "retry.policy", "retry.policy"]);
			});

			it("should success the job after 3 retries (from retryPolicy)", async () => {
				const job = await broker.wf.run("retry.policy", {});
				expect(job.id).toEqual(expect.any(String));
				const result = await job.promise();
				expect(result).toBe("Success on attempt 3");
				expect(FLOWS).toEqual([
					"retry.policy",
					"retry.policy",
					"retry.policy",
					"retry.policy"
				]);
			});
		});
	});
});
