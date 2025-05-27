import "moleculer";

import { WorkflowContextProps, WorkflowHandler, WorkflowServiceBrokerMethods } from "./types.ts";
import Workflow, { WorkflowSchema } from "./workflow.ts";

declare module "moleculer" {
	interface Context {
		wf: WorkflowContextProps;
	}

	interface ServiceBroker {
		wf: WorkflowServiceBrokerMethods;
	}

	interface Service {
		$workflows?: Workflow[];
	}

	interface ServiceSchema {
		workflows?: {
			[key: string]: WorkflowSchema;
		};
	}

	interface Middleware {
		localWorkflow?: (next: WorkflowHandler) => WorkflowHandler;
	}
}
