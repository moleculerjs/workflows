const ServiceBroker = require("moleculer").ServiceBroker;
const RedisAdapter = require("../../src/adapters/redis");
const C = require("../../src/constants");

describe("RedisAdapter.getKey without custom prefix", () => {
	const broker = new ServiceBroker({ logger: false });
	const adapter = new RedisAdapter();
	adapter.init(broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("molwf:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("molwf:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("molwf:wf1:job:123");
	});
});

describe("RedisAdapter.getKey with broker namespace", () => {
	const broker = new ServiceBroker({ logger: false, namespace: "ns1" });
	const adapter = new RedisAdapter();
	adapter.init(broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("molwf-ns1:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("molwf-ns1:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("molwf-ns1:wf1:job:123");
	});
});

describe("RedisAdapter.getKey with custom prefix", () => {
	const broker = new ServiceBroker({ logger: false, namespace: "ns1" });
	const adapter = new RedisAdapter({ prefix: "custom" });
	adapter.init(broker, broker.logger, {});

	it(`should generate key without type and id`, () => {
		expect(adapter.getKey("wf1")).toBe("custom:wf1");
	});

	it(`should generate key with type`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_WAITING)).toBe("custom:wf1:waiting");
	});

	it(`should generate key with type and id`, () => {
		expect(adapter.getKey("wf1", C.QUEUE_JOB, "123")).toBe("custom:wf1:job:123");
	});
});
