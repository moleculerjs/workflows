import "moleculer";

import { WorkflowContextProps, WorkflowServiceBrokerMethods } from "./types.ts";

declare module "moleculer" {
	interface Context {
		wf?: WorkflowContextProps;
	}

	interface ServiceBroker {
		wf?: WorkflowServiceBrokerMethods;
	}
}
