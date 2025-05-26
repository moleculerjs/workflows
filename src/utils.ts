import { CronExpressionParser } from "cron-parser";

/**
 * Credits: https://github.com/jkroso/parse-duration
 */

const durationRE =
	/((?:\d{1,16}(?:\.\d{1,16})?|\.\d{1,16})(?:[eE][-+]?\d{1,4})?)\s?([\p{L}]{0,14})/gu;

const unit = Object.create(null);
const m = 60000,
	h = m * 60,
	d = h * 24,
	y = d * 365.25;

unit.year = unit.yr = unit.y = y;
unit.month = unit.mo = unit.mth = y / 12;
unit.week = unit.wk = unit.w = d * 7;
unit.day = unit.d = d;
unit.hour = unit.hr = unit.h = h;
unit.minute = unit.min = unit.m = m;
unit.second = unit.sec = unit.s = 1000;
unit.millisecond = unit.millisec = unit.ms = 1;
unit.microsecond = unit.microsec = unit.us = unit.µs = 1e-3;
unit.nanosecond = unit.nanosec = unit.ns = 1e-6;

unit.group = ",";
unit.decimal = ".";
unit.placeholder = " _";

/**
 * Parse a duration string into a numeric value based on the specified format.
 *
 * @param {string|number} str The duration string to parse.
 * @param {string} [format="ms"] The format to return the duration in (e.g., "ms", "s").
 * @returns {number|null} The parsed duration in the specified format, or null if invalid.
 */
function parseDuration(str: string | number, format = "ms"): number | null {
	if (str == null) {
		return null;
	}

	let result = null,
		prevUnits;

	String(str)
		.replace(new RegExp(`(\\d)[${unit.placeholder}${unit.group}](\\d)`, "g"), "$1$2") // clean up group separators / placeholders
		.replace(unit.decimal, ".") // normalize decimal separator
		// @ts-expect-error no-overload-method
		.replace(durationRE, (_, n, units) => {
			// if no units, find next smallest units or fall back to format value
			// eg. 1h30 -> 1h30m
			if (!units) {
				if (prevUnits) {
					for (const u in unit)
						if (unit[u] < prevUnits) {
							units = u;
							break;
						}
				} else units = format;
			} else units = units.toLowerCase();

			prevUnits = units = unit[units] || unit[units.replace(/s$/, "")];

			if (units) result = (result || 0) + n * units;
		});

	return result && (result / (unit[format] || 1)) * (str[0] === "-" ? -1 : 1);
}

/**
 * Generate a random value around the given number, reducing it by up to 25% and adding a random offset.
 *
 * @param {number} x The base value to randomize.
 * @returns {number} The randomized value.
 */
function circa(x: number): number {
	const h = x / 2;
	const y = Math.floor(Math.random() * h);

	return x - x / 4 + y;
}

const units = ["h", "m", "s", "ms", "μs", "ns"];
const divisors = [60 * 60 * 1000, 60 * 1000, 1000, 1, 1e-3, 1e-6];

/**
 * Convert a duration in milliseconds into a human-readable string.
 *
 * @param milli The duration in milliseconds.
 * @returns The human-readable duration (e.g., "1h", "30m").
 */
function humanize(milli: number | null): string {
	if (milli == null) return "?";

	for (let i = 0; i < divisors.length; i++) {
		const val = milli / divisors[i];
		if (val >= 1.0) return "" + Math.floor(val) + units[i];
	}

	return "now";
}

/**
 * Calculate the next execution time for a given cron expression.
 *
 * @param cron The cron expression to evaluate.
 * @param currentDate The current date to base the calculation on.
 * @param tz The timezone to use for the calculation.
 * @returns The timestamp of the next execution time.
 */
function getCronNextTime(cron: string, currentDate?: number, tz?: string): number {
	const interval = CronExpressionParser.parse(cron, {
		currentDate,
		tz
	});

	const next = interval.next();

	return next.getTime();
}

export { parseDuration, circa, humanize, getCronNextTime };
