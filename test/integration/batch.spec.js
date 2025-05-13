const { ServiceBroker } = require("moleculer");
const WorkflowsMiddleware = require("../../src");
const _ = require("lodash");

describe("Workflows Batch Test", () => {
	let broker;

	const cleanup = async () => {
		await broker.wf.cleanup("batch.single");
		await broker.wf.cleanup("batch.multi");
	};

	beforeAll(async () => {
		broker = new ServiceBroker({
			logger: false,
			middlewares: [WorkflowsMiddleware({ adapter: "Redis" })]
		});

		broker.createService({
			name: "batch",
			workflows: {
				single: {
					async handler(ctx) {
						return `Single called`;
					}
				},
				multi: {
					concurrency: 10,
					async handler(ctx) {
						return `Multi called`;
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

	it("should execute 1000 workflows (single)", async () => {
		const promises = [];
		for (let i = 0; i < 1000; i++) {
			promises.push(broker.wf.run("batch.single", { i }));
		}

		const results = await Promise.all(promises.map(p => p.then(job => job.promise())));
		expect(results.length).toBe(1000);
		expect(results[0]).toBe("Single called");
		expect(_.uniq(results).length).toBe(1);
	});

	it("should execute 1000 workflows (multi)", async () => {
		const promises = [];
		for (let i = 0; i < 1000; i++) {
			promises.push(broker.wf.run("batch.multi", { i }));
		}

		const results = await Promise.all(promises.map(p => p.then(job => job.promise())));
		expect(results.length).toBe(1000);
		expect(results[0]).toBe("Multi called");
		expect(_.uniq(results).length).toBe(1);
	});
});
