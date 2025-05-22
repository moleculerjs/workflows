/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const BaseAdapter = require("./base");
const C = require("../constants");

/**
 * @typedef {import("moleculer").ServiceBroker} ServiceBroker Moleculer Service Broker instance
 * @typedef {import("moleculer").Context} Context Context instance
 * @typedef {import("moleculer").Service} Service Service instance
 * @typedef {import("moleculer").LoggerInstance} Logger Logger instance
 * @typedef {import("../index").Workflow} Workflow
 * @typedef {import("../index").WorkflowsMiddlewareOptions} WorkflowsMiddlewareOptions
 */

/**
 * @typedef {Object} FakeOptions Fake Adapter configuration
 */

/**
 * Fake (Moleculer Event-based) adapter
 *
 * @class FakeAdapter
 * @extends {BaseAdapter}
 */
class FakeAdapter extends BaseAdapter {
	/**
	 * Constructor of adapter.
	 *
	 * @param  {Object?} opts
	 */
	constructor(opts) {
		if (_.isString(opts)) opts = {};

		super(opts);
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {Workflow} wf
	 * @param {ServiceBroker} broker
	 * @param {Logger} logger
	 * @param {WorkflowsMiddlewareOptions} mwOpts - Middleware options.
	 */
	init(wf, broker, logger, mwOpts) {
		super.init(wf, broker, logger, mwOpts);
	}

	/**
	 * Connect to the adapter.
	 */
	async connect() {
		this.connected = true;
	}

	/**
	 * Disconnect from adapter
	 */
	async disconnect() {
		this.connected = false;
	}
}

module.exports = FakeAdapter;
