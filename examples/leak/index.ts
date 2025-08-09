/* eslint-disable @/no-console */
"use strict";

/**
 * It's a test to check memory leak
 */

import { ServiceBroker, Context } from "moleculer";
import process from "node:process";

import { Middleware } from "../../src/index.ts";
import kleur from "kleur";

const MAX_COUNT = 10_000_000;
let executed = 0;

// Create broker
const broker = new ServiceBroker({
	transporter: "Redis",
	logger: true,
	logLevel: "warn",

	middlewares: [Middleware({})]
});

// Create a service
broker.createService({
	name: "test",

	// Define workflows
	workflows: {
		// Test 1 workflow.
		wf: {
			concurrency: 100,
			removeOnCompleted: true,
			removeOnFailed: true,

			// Workflow handler
			async handler(ctx: Context) {
				await ctx.wf.setState("start");

				executed++;

				return true;
			}
		}
	}
});

function signed(value: number): string {
	let unit = "bytes";
	if (Math.abs(value) >= 1024) {
		unit = "kBytes";
		value = Math.floor(value / 1024); // Convert to kByte
	}
	if (Math.abs(value) >= 1024) {
		unit = "MBytes";
		value = Math.floor(value / 1024); // Convert to MByte
	}

	if (value > 0) {
		return kleur.bold().red(`+${value} ${unit}`);
	} else if (value < 0) {
		return kleur.bold().green(`-${Math.abs(value)} ${unit}`);
	} else {
		return `0 ${unit}`;
	}
}

// Start server
broker
	.start()
	.then(async () => {
		console.log("Waiting before test...");
		await new Promise(resolve => setTimeout(resolve, 10_000));
		console.log(`Starting memory leak test... count: ${MAX_COUNT}`);
		if (global.gc) {
			console.log("Run GC...");
			global.gc();
		}
		const beforeMemory = process.memoryUsage();
		console.log("Memory usage before test:", beforeMemory);
		for (let i = 0; i < MAX_COUNT; i++) {
			await broker.wf.run("test.wf", { i });
			if (i % 1000 === 0) {
				process.stdout.write(".");
			}
		}
		console.log("\nWaiting for jobs finishing...");
		await new Promise(resolve => setTimeout(resolve, 10_000));
		console.log("Test completed. Executed: ", executed);
		if (global.gc) {
			console.log("Run GC...");
			global.gc();
		}
		const afterMemory = process.memoryUsage();
		console.log("Memory usage after test:", afterMemory);

		console.log("DIFF rss:", signed(afterMemory.rss - beforeMemory.rss));
		console.log("DIFF heapTotal:", signed(afterMemory.heapTotal - beforeMemory.heapTotal));
		console.log("DIFF heapUsed:", signed(afterMemory.heapUsed - beforeMemory.heapUsed));
		console.log("DIFF external:", signed(afterMemory.external - beforeMemory.external));
		console.log(
			"DIFF arrayBuffers:",
			signed(afterMemory.arrayBuffers - beforeMemory.arrayBuffers)
		);

		await broker.stop();
		process.exit(0);
	})
	.catch(err => {
		broker.logger.error(err);
		process.exit(1);
	});
