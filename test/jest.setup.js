const minDate = new Date("2020-01-01T00:00:00Z").getTime();
const maxDate = new Date("2030-01-01T00:00:00Z").getTime();

expect.extend({
	withinRange(actual, min, max) {
		if (typeof actual !== "number") {
			throw new Error("Actual value must be a number");
		}
		const pass = actual >= min && actual <= max;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not to be within range (${min}..${max})`
				: () => `expected ${actual} to be within range (${min}..${max})`
		};
	},

	greaterThan(actual, min) {
		if (typeof actual !== "number") {
			throw new Error("Actual value must be a number");
		}
		const pass = actual > min;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not less than ${min}`
				: () => `expected ${actual} to be greater than ${min}`
		};
	},

	greaterThanOrEqual(actual, min) {
		if (typeof actual !== "number") {
			throw new Error("Actual value must be a number");
		}
		const pass = actual >= min;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not less than ${min}`
				: () => `expected ${actual} to be greater than or equal ${min}`
		};
	},

	epoch(actual) {
		if (typeof actual !== "number") {
			throw new Error("Actual value must be a number");
		}
		const pass = actual > minDate && actual < maxDate;

		return {
			pass,
			message: pass
				? () => `expected ${actual} not to be a valid epoch`
				: () => `expected ${actual} to be a valid epoch`
		};
	},

	toBeItemAfter(array, item, afterItem) {
		if (!Array.isArray(array)) {
			throw new Error("First argument must be an array");
		}

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
