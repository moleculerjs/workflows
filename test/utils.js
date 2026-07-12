"use strict";

module.exports = {
	delay(ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	},

	// The adapter type used by the integration tests. Set the WF_TEST_ADAPTER
	// environment variable to run the tests with another adapter (e.g. "Fake").
	adapterType: process.env.WF_TEST_ADAPTER || "Redis",

	// The broker transporter used by the multi-broker integration tests.
	// With the Fake adapter the whole test suite runs without Redis.
	transporterType: process.env.WF_TEST_ADAPTER === "Fake" ? "Fake" : "Redis"
};
