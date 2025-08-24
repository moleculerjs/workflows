import { describe, expect, it } from "vitest";
import { ServiceBroker } from "moleculer";
import FakeAdapter from "../../src/adapters/fake.ts";
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
		// expect(adapter.getSignalKey("test.signal", 123)).toBe("molwf:signals:test.signal:123");
	});
});

describe("FakeAdapter.getKey with namespace", () => {
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
		// expect(adapter.getSignalKey("test.signal", 123)).toBe("molwf-ns1:signals:test.signal:123");
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
		// expect(adapter.getSignalKey("test.signal", 123)).toBe("custom:signals:test.signal:123");
	});
});