import { describe, expect, it } from "vitest";
import { Errors } from "moleculer";
import { Adapters } from "../../src/index.ts";

describe("Test Adapter resolver", () => {
	it("should resolve null to Redis adapter", () => {
		const adapter = Adapters.resolve();
		expect(adapter).toBeInstanceOf(Adapters.Redis);
		expect(adapter.opts).toEqual({
			drainDelay: 5,
			redis: { retryStrategy: expect.any(Function) },
			serializer: "JSON"
		});
	});

	/*
	describe("Resolve Fake adapter", () => {
		it("should resolve Fake adapter from string", () => {
			const adapter = Adapters.resolve("Fake");
			expect(adapter).toBeInstanceOf(Adapters.Fake);
		});

		it("should resolve Fake adapter from obj with Fake type", () => {
			const options = { drainDelay: 10 };
			const adapter = Adapters.resolve({ type: "Fake", options });
			expect(adapter).toBeInstanceOf(Adapters.Fake);
			expect(adapter.opts).toMatchObject({ drainDelay: 10 });
		});
	});
	*/

	describe("Resolve Redis adapter", () => {
		it("should resolve Redis adapter from connection string", () => {
			const adapter = Adapters.resolve("redis://localhost");
			expect(adapter).toBeInstanceOf(Adapters.Redis);
			expect(adapter.opts).toMatchObject({ redis: { url: "redis://localhost" } });
		});

		it("should resolve Redis adapter from SSL connection string", () => {
			const adapter = Adapters.resolve("rediss://localhost");
			expect(adapter).toBeInstanceOf(Adapters.Redis);
			expect(adapter.opts).toMatchObject({ redis: { url: "rediss://localhost" } });
		});

		it("should resolve Redis adapter from string", () => {
			const adapter = Adapters.resolve("Redis");
			expect(adapter).toBeInstanceOf(Adapters.Redis);
		});

		it("should resolve Redis adapter from obj with Redis type", () => {
			const options = { drainDelay: 10 };
			const adapter = Adapters.resolve({ type: "Redis", options });
			expect(adapter).toBeInstanceOf(Adapters.Redis);
			expect(adapter.opts).toMatchObject({ drainDelay: 10 });
		});
	});

	it("should throw error if type if not correct", () => {
		expect(() => {
			Adapters.resolve("xyz");
		}).toThrowError(Errors.ServiceSchemaError);

		expect(() => {
			// @ts-expect-error Invalid type
			Adapters.resolve({ type: "xyz" });
		}).toThrowError(Errors.ServiceSchemaError);
	});
});
