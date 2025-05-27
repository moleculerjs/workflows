/* eslint-disable @typescript-eslint/no-empty-object-type */

interface CustomMatchers<T = unknown> {
	withinRange(min: number, max: number): T;
	greaterThan(min: number): T;
	greaterThanOrEqual(min: number): T;
	epoch(): T;
	toBeItemAfter(array: unknown[], item: unknown, afterItem: unknown): T;
}

declare module "vitest" {
	interface Assertion<T = unknown> extends CustomMatchers<T> {}
	interface AsymmetricMatchersContaining extends CustomMatchers {}
}
