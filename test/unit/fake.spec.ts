import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { ServiceBroker } from "moleculer";
import FakeAdapter, { FakeStorage } from "../../src/adapters/fake.ts";
import * as C from "../../src/constants.ts";

describe("FakeAdapter.getKey without custom prefix", () => {
	const broker = new ServiceBroker({ logger: false });
	const adapter = new FakeAdapter();
	adapter.init(null, broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("molwf:workflows:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("molwf:workflows:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("molwf:workflows:wf1:job:123");
	});

	it(`should generate signal key`, () => {
		expect(adapter.getSignalKey("test.signal", "123")).toBe("molwf:signals:test.signal:123");
	});
});

describe("FakeAdapter.getKey with broker namespace", () => {
	const broker = new ServiceBroker({ logger: false, namespace: "ns1" });
	const adapter = new FakeAdapter();
	adapter.init(null, broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("molwf-ns1:workflows:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("molwf-ns1:workflows:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("molwf-ns1:workflows:wf1:job:123");
	});

	it(`should generate signal key`, () => {
		expect(adapter.getSignalKey("test.signal", "123")).toBe(
			"molwf-ns1:signals:test.signal:123"
		);
	});
});

describe("FakeAdapter.getKey with custom prefix", () => {
	const broker = new ServiceBroker({ logger: false, namespace: "ns1" });
	const adapter = new FakeAdapter({ prefix: "custom" });
	adapter.init(null, broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("custom:workflows:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("custom:workflows:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("custom:workflows:wf1:job:123");
	});

	it(`should generate signal key`, () => {
		expect(adapter.getSignalKey("test.signal", "123")).toBe("custom:signals:test.signal:123");
	});
});

describe("FakeAdapter shared storage", () => {
	afterEach(() => {
		FakeAdapter.clearAll();
	});

	it("should share the storage between instances with the same prefix", async () => {
		const broker = new ServiceBroker({ logger: false });
		const adapter1 = new FakeAdapter();
		const adapter2 = new FakeAdapter();
		adapter1.init(null, broker, broker.logger, {});
		adapter2.init(null, broker, broker.logger, {});
		await adapter1.connect();
		await adapter2.connect();

		expect(adapter1.storage).toBe(adapter2.storage);

		adapter1.storage.hset("key1", "field1", "value1");
		expect(adapter2.storage.hget("key1", "field1")).toBe("value1");
	});

	it("should isolate storages with different prefixes", async () => {
		const broker = new ServiceBroker({ logger: false });
		const adapter1 = new FakeAdapter({ prefix: "first" });
		const adapter2 = new FakeAdapter({ prefix: "second" });
		adapter1.init(null, broker, broker.logger, {});
		adapter2.init(null, broker, broker.logger, {});
		await adapter1.connect();
		await adapter2.connect();

		expect(adapter1.storage).not.toBe(adapter2.storage);

		adapter1.storage.hset("key1", "field1", "value1");
		expect(adapter2.storage.hget("key1", "field1")).toBe(null);
	});

	it("should keep the storage data after disconnect", async () => {
		const broker = new ServiceBroker({ logger: false });
		const adapter1 = new FakeAdapter();
		adapter1.init(null, broker, broker.logger, {});
		await adapter1.connect();
		adapter1.storage.hset("key1", "field1", "value1");
		await adapter1.disconnect();

		const adapter2 = new FakeAdapter();
		adapter2.init(null, broker, broker.logger, {});
		await adapter2.connect();
		expect(adapter2.storage.hget("key1", "field1")).toBe("value1");
	});
});

describe("FakeStorage", () => {
	let storage: FakeStorage;

	beforeEach(() => {
		storage = new FakeStorage();
	});

	describe("hash commands", () => {
		it("should set and get hash fields", () => {
			storage.hmset("h1", { a: 1, b: "two", c: true });
			expect(storage.hget("h1", "a")).toBe("1");
			expect(storage.hget("h1", "b")).toBe("two");
			expect(storage.hget("h1", "c")).toBe("true");
			expect(storage.hget("h1", "missing")).toBe(null);
			expect(storage.hmget("h1", ["a", "missing", "b"])).toEqual(["1", null, "two"]);
			expect(storage.hgetall("h1")).toEqual({ a: "1", b: "two", c: "true" });
		});

		it("should skip undefined values", () => {
			storage.hmset("h1", { a: 1, b: undefined });
			expect(storage.hgetall("h1")).toEqual({ a: "1" });
		});

		it("should keep Buffer values", () => {
			const buf = Buffer.from("data");
			storage.hset("h1", "a", buf);
			expect(storage.hget("h1", "a")).toBe(buf);
		});

		it("should set field only if not exists with hsetnx", () => {
			expect(storage.hsetnx("h1", "a", "1")).toBe(1);
			expect(storage.hsetnx("h1", "a", "2")).toBe(0);
			expect(storage.hget("h1", "a")).toBe("1");
		});

		it("should increment field with hincrby", () => {
			expect(storage.hincrby("h1", "counter", 1)).toBe(1);
			expect(storage.hincrby("h1", "counter", 2)).toBe(3);
			expect(storage.hget("h1", "counter")).toBe("3");
		});

		it("should delete fields with hdel", () => {
			storage.hmset("h1", { a: 1, b: 2 });
			storage.hdel("h1", ["a"]);
			expect(storage.hgetall("h1")).toEqual({ b: "2" });
			storage.hdel("h1", ["b"]);
			expect(storage.exists("h1")).toBe(false);
		});
	});

	describe("list commands", () => {
		it("should push and range like Redis", () => {
			storage.lpush("l1", "a");
			storage.lpush("l1", "b");
			storage.rpush("l1", "c");
			expect(storage.lrange("l1", 0, -1)).toEqual(["b", "a", "c"]);
		});

		it("should pop from tail and push to head with rpoplpush", () => {
			storage.lpush("l1", "a");
			storage.lpush("l1", "b");
			expect(storage.rpoplpush("l1", "l2")).toBe("a");
			expect(storage.lrange("l1", 0, -1)).toEqual(["b"]);
			expect(storage.lrange("l2", 0, -1)).toEqual(["a"]);
			expect(storage.rpoplpush("empty", "l2")).toBe(null);
		});

		it("should remove items with lrem", () => {
			storage.rpush("l1", "a");
			storage.rpush("l1", "b");
			storage.rpush("l1", "a");
			expect(storage.lrem("l1", 1, "a")).toBe(1);
			expect(storage.lrange("l1", 0, -1)).toEqual(["b", "a"]);
			expect(storage.lrem("l1", 0, "x")).toBe(0);
		});
	});

	describe("sorted set commands", () => {
		it("should order entries by score", () => {
			storage.zadd("z1", 30, "c");
			storage.zadd("z1", 10, "a");
			storage.zadd("z1", 20, "b");
			expect(storage.zrange("z1", 0, -1)).toEqual(["a", "b", "c"]);
			expect(storage.zrange("z1", 0, 0)).toEqual(["a"]);
			expect(storage.zrangeWithScores("z1", 0, -1)).toEqual([
				"a",
				"10",
				"b",
				"20",
				"c",
				"30"
			]);
		});

		it("should order ties by member name", () => {
			storage.zadd("z1", 10, "b");
			storage.zadd("z1", 10, "a");
			expect(storage.zrange("z1", 0, -1)).toEqual(["a", "b"]);
		});

		it("should filter by score with zrangebyscore", () => {
			storage.zadd("z1", 10, "a");
			storage.zadd("z1", 20, "b");
			storage.zadd("z1", 30, "c");
			expect(storage.zrangebyscore("z1", 0, 20)).toEqual(["a", "b"]);
			expect(storage.zrangebyscore("z1", 15, 100)).toEqual(["b", "c"]);
		});

		it("should remove by score with zremrangebyscore", () => {
			storage.zadd("z1", 10, "a");
			storage.zadd("z1", 20, "b");
			storage.zremrangebyscore("z1", 0, 15);
			expect(storage.zrange("z1", 0, -1)).toEqual(["b"]);
		});
	});

	describe("string commands with expiration", () => {
		afterEach(() => {
			vi.useRealTimers();
		});

		it("should set and get string values", () => {
			expect(storage.setString("s1", "v1")).toBe("OK");
			expect(storage.getString("s1")).toBe("v1");
			expect(storage.getString("missing")).toBe(null);
		});

		it("should honor NX and XX flags", () => {
			expect(storage.setString("s1", "v1", { nx: true })).toBe("OK");
			expect(storage.setString("s1", "v2", { nx: true })).toBe(null);
			expect(storage.getString("s1")).toBe("v1");

			expect(storage.setString("s2", "v1", { xx: true })).toBe(null);
			expect(storage.setString("s1", "v2", { xx: true })).toBe("OK");
			expect(storage.getString("s1")).toBe("v2");
		});

		it("should expire values lazily", () => {
			vi.useFakeTimers();
			storage.setString("s1", "v1", { px: 1000 });
			expect(storage.getString("s1")).toBe("v1");
			expect(storage.exists("s1")).toBe(true);

			vi.advanceTimersByTime(1500);
			expect(storage.getString("s1")).toBe(null);
			expect(storage.exists("s1")).toBe(false);

			// NX should succeed after expiration
			expect(storage.setString("s1", "v2", { nx: true })).toBe("OK");
		});
	});

	describe("generic commands", () => {
		it("should check existence across all stores", () => {
			storage.hset("h1", "a", 1);
			storage.lpush("l1", "a");
			storage.sadd("st1", ["a"]);
			storage.zadd("z1", 1, "a");
			storage.setString("s1", "a");

			for (const key of ["h1", "l1", "st1", "z1", "s1"]) {
				expect(storage.exists(key)).toBe(true);
			}
			expect(storage.exists("missing")).toBe(false);
		});

		it("should delete keys from all stores", () => {
			storage.hset("k", "a", 1);
			storage.lpush("k", "a");
			storage.del("k");
			expect(storage.exists("k")).toBe(false);
		});

		it("should list all keys", () => {
			storage.hset("h1", "a", 1);
			storage.lpush("l1", "a");
			storage.setString("s1", "a");
			expect(storage.keys().sort()).toEqual(["h1", "l1", "s1"]);
		});
	});
});

describe("FakeAdapter.popWaitingJob", () => {
	afterEach(() => {
		FakeAdapter.clearAll();
	});

	async function createAdapter() {
		const broker = new ServiceBroker({ logger: false });
		const adapter = new FakeAdapter();
		const fakeWf = { name: "wf1", opts: { concurrency: 1 } };
		adapter.init(fakeWf as never, broker, broker.logger, {});
		await adapter.connect();
		adapter.running = true;
		return adapter;
	}

	it("should pop immediately if the waiting queue is not empty", async () => {
		const adapter = await createAdapter();
		adapter.storage.lpush(adapter.getKey("wf1", C.QUEUE_WAITING), "job1");

		const jobId = await adapter.popWaitingJob(1);
		expect(jobId).toBe("job1");
		expect(adapter.storage.lrange(adapter.getKey("wf1", C.QUEUE_ACTIVE), 0, -1)).toEqual([
			"job1"
		]);
	});

	it("should return null after the timeout", async () => {
		const adapter = await createAdapter();

		const start = Date.now();
		const jobId = await adapter.popWaitingJob(0.2);
		expect(jobId).toBe(null);
		expect(Date.now() - start).toBeGreaterThanOrEqual(150);
	});

	it("should wake up when a new job is pushed", async () => {
		const adapter = await createAdapter();

		const promise = adapter.popWaitingJob(5);
		setTimeout(() => {
			adapter.storage.lpush(adapter.getKey("wf1", C.QUEUE_WAITING), "job2");
			adapter.wakeUpWorkers("wf1");
		}, 100);

		const start = Date.now();
		const jobId = await promise;
		expect(jobId).toBe("job2");
		expect(Date.now() - start).toBeLessThan(1000);
	});
});
