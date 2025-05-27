/*
 * @moleculer/workflows
 * Copyright (c) 2025 MoleculerJS (https://github.com/moleculerjs/workflows)
 * MIT Licensed
 */

"use strict";

import _ from "lodash";
import { Errors } from "moleculer";
import BaseAdapter, { BaseDefaultOptions } from "./base.ts";
import RedisAdapter, { RedisAdapterOptions } from "./redis.ts";

const Adapters = {
	Base: BaseAdapter,
	// Fake: require("./fake"),
	Redis: RedisAdapter
};

type AdapterTypes = (typeof Adapters)[keyof typeof Adapters];

type ResolvableAdapterType =
	| keyof typeof Adapters
	| BaseAdapter
	| { type: keyof typeof Adapters; options: BaseDefaultOptions | RedisAdapterOptions };

function getByName(name: string): AdapterTypes | null {
	if (!name) return null;

	const n = Object.keys(Adapters).find(n => n.toLowerCase() == name.toLowerCase());
	if (n) return Adapters[n];
}

/**
 * Resolve adapter by name
 *
 * @param opt
 */
function resolve(opt: ResolvableAdapterType): BaseAdapter {
	if (opt instanceof BaseAdapter) {
		return opt;
	} else if (_.isString(opt)) {
		const AdapterClass = getByName(opt);
		if (AdapterClass) {
			return new AdapterClass();
		} else if (opt.startsWith("redis://") || opt.startsWith("rediss://")) {
			return new Adapters.Redis(opt);
		} else {
			throw new Errors.ServiceSchemaError(`Invalid Adapter type '${opt}'.`, { type: opt });
		}
	} else if (_.isObject(opt)) {
		const AdapterClass = getByName(opt.type || "Redis");
		if (AdapterClass) {
			return new AdapterClass(opt.options);
		} else {
			throw new Errors.ServiceSchemaError(`Invalid Adapter type '${opt.type}'.`, {
				type: opt.type
			});
		}
	}

	return new Adapters.Redis();
}

/**
 * Register a new Channel Adapter
 *
 * @param name
 * @param value
 */
function register(name: string, value: BaseAdapter) {
	Adapters[name] = value;
}

export default Object.assign(Adapters, { resolve, register });
