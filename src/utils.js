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

function parseDuration(str, format = "ms") {
	if (str == null) {
		return str;
	}

	let result = null,
		prevUnits;

	String(str)
		.replace(new RegExp(`(\\d)[${unit.placeholder}${unit.group}](\\d)`, "g"), "$1$2") // clean up group separators / placeholders
		.replace(unit.decimal, ".") // normalize decimal separator
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

function circa(x) {
	const h = x / 2;
	const y = Math.floor(Math.random() * h);

	return x - x / 4 + y;
}

module.exports = {
	parseDuration,
	circa
};
