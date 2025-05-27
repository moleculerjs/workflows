/* eslint-disable @typescript-eslint/no-empty-object-type */
import { expect } from "vitest";

const minDate = new Date("2020-01-01T00:00:00Z").getTime();
const maxDate = new Date("2030-01-01T00:00:00Z").getTime();

expect.extend({
	withinRange(actual, min, max) {
		const pass = actual >= min && actual <= max;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not to be within range (${min}..${max})`
				: () => `expected ${actual} to be within range (${min}..${max})`
		};
	},

	greaterThan(actual, min) {
		const pass = actual > min;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not less than ${min}`
				: () => `expected ${actual} to be greater than ${min}`
		};
	},

	greaterThanOrEqual(actual, min) {
		const pass = actual >= min;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not less than ${min}`
				: () => `expected ${actual} to be greater than or equal ${min}`
		};
	},

	epoch(actual) {
		const pass = actual > minDate && actual < maxDate;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not to be a valid epoch`
				: () => `expected ${actual} to be a valid epoch`
		};
	},

	toBeItemAfter(array, item, afterItem) {
		const itemIndex = array.indexOf(item);
		const afterItemIndex = array.indexOf(afterItem);
		if (itemIndex === -1 || afterItemIndex === -1) {
			throw new Error("Item or afterItem not found in array");
		}
		const pass = itemIndex > afterItemIndex;
		return {
			pass,
			message: pass
				? () => `expected ${item} not to be after ${afterItem}`
				: () => `expected ${item} to be after ${afterItem}`
		};
	}
});

interface CustomMatchers<T = unknown> {
	withinRange(min: number, max: number): T;
	greaterThan(min: number): T;
	greaterThanOrEqual(min: number): T;
	epoch(): T;
	toBeItemAfter(array: unknown[], item: unknown, afterItem: unknown): T;
}

declare module "vitest" {
	interface Assertion extends CustomMatchers {}
	interface AsymmetricMatchersContaining extends CustomMatchers {}
}
