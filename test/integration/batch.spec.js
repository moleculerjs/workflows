const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const _ = require("lodash");
require("../jest.setup.js");

describe("Workflows Batch Test (on single node)", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanUp("batch.serial");
		await broker.wf.cleanUp("batch.parallel");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis", schemaProperty: "WF" })]
		});

		broker.createService({
			name: "batch",
			WF: {
				serial: {
					async handler() {
						return `Serial called`;
					}
				},
				parallel: {
					concurrency: 10,
					async handler() {
						return `Parallel called`;
					}
				}
			}
		});

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		// await (await broker.wf.getAdapter()).dumpWorkflows("./tmp", ["batch.serial", "batch.parallel"]);
		await cleanup();
		await broker.stop();
	});

	it("should execute 1000 workflows (serial)", async () => {
		const promises = [];
		for (let i = 0; i < 1000; i++) {
			promises.push(broker.wf.run("batch.serial", { i }));
		}

		const results = await Promise.all(promises.map(p => p.then(job => job.promise())));
		expect(results.length).toBe(1000);
		expect(results[0]).toBe("Serial called");
		expect(_.uniq(results).length).toBe(1);
	});

	it("should execute 1000 workflows (parallel)", async () => {
		const promises = [];
		for (let i = 0; i < 1000; i++) {
			promises.push(broker.wf.run("batch.parallel", { i }));
		}

		const results = await Promise.all(promises.map(p => p.then(job => job.promise())));
		expect(results.length).toBe(1000);
		expect(results[0]).toBe("Parallel called");
		expect(_.uniq(results).length).toBe(1);
	});
});

describe("Workflows Batch Test (on multiple nodes)", () => {
	let broker;
	let workers = [];

	const cleanup = async () => {
		await broker.wf.cleanUp("batch.multi");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		for (let i = 0; i < 5; i++) {
			const worker = new ServiceBroker({
				logger: false,
				nodeID: "worker-" + i,
				middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
			});

			worker.createService({
				name: "batch",
				workflows: {
					multi: {
						async handler() {
							return `Worker ${this.broker.nodeID} called`;
						}
					}
				}
			});

			await worker.start();
			workers.push(worker);
		}

		await broker.start();
		await cleanup();
	});

	afterAll(async () => {
		await cleanup();
		await broker.stop();
		for (const worker of workers) {
			await worker.stop();
		}
	});

	it("should execute 5000 workflows (multi-worker)", async () => {
		const promises = [];
		for (let i = 0; i < 5000; i++) {
			promises.push(broker.wf.run("batch.multi", { i }));
		}

		const results = await Promise.all(promises.map(p => p.then(job => job.promise())));
		expect(results.length).toBe(5000);
		//expect(results[0]).toBe("Multi worker called");
		expect(_.uniq(results).length).toBe(5);

		const countByWorkers = Object.fromEntries(
			Object.entries(_.groupBy(results)).map(([k, v]) => [k, v.length])
		);

		// console.log("Results by worker:", countByWorkers);

		Object.keys(countByWorkers).forEach(worker => {
			expect(countByWorkers[worker]).withinRange(980, 1200);
		});
	}, 10_000);
});
