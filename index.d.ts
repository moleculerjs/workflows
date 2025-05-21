import { Middleware } from "moleculer";
import { BaseAdapter, RedisAdapter } from "./adapters";

export const Middleware = (mwOpts: WorkflowsMiddlewareOptions) => Middleware;

export const Adapters = {
	Base: BaseAdapter,
	Redis: RedisAdapter,
};
