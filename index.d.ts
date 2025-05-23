import { Middleware as MW } from "moleculer";
import { BaseAdapter, RedisAdapter } from "./adapters";

export const Middleware = (mwOpts: WorkflowsMiddlewareOptions) => MW;

export const Adapters = {
	Base: BaseAdapter,
	Redis: RedisAdapter,
};
