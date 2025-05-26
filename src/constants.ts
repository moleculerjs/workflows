export const SIGNAL_EMPTY_KEY = "null";

export const JOB_FIELDS_JSON: string[] = ["payload", "repeat", "result", "error", "state"];

export const JOB_FIELDS_NUMERIC: string[] = [
	"createdAt",
	"startedAt",
	"finishedAt",
	"promoteAt",
	"repeatCounter",
	"stalledCounter",
	"delay",
	"timeout",
	"duration",
	"retries",
	"retryAttempts"
];

export const JOB_FIELDS_BOOLEAN: string[] = ["success"];
// export const JOB_FIELDS_STRING: string[] = ["id", "parent", "nodeID"];
// export const JOB_FIELDS: string[] = [...JOB_FIELDS_STRING, ...JOB_FIELDS_JSON, ...JOB_FIELDS_NUMERIC, ...JOB_FIELDS_BOOLEAN];

export const RERUN_REMOVABLE_FIELDS: string[] = [
	"startedAt",
	"duration",
	"finishedAt",
	"nodeID",
	"success",
	"error",
	"result"
];

export const QUEUE_CATEGORY_WF = "workflows";
export const QUEUE_CATEGORY_SIGNAL = "signals";
export const QUEUE_JOB = "job";
export const QUEUE_JOB_LOCK = "job-lock";
export const QUEUE_JOB_EVENTS = "job-events";
export const QUEUE_MAINTENANCE_LOCK = "maintenance-lock";
export const QUEUE_MAINTENANCE_LOCK_DELAYED = "maintenance-lock-delayed";
export const QUEUE_WAITING = "waiting";
export const QUEUE_ACTIVE = "active";
export const QUEUE_STALLED = "stalled";
export const QUEUE_DELAYED = "delayed";
export const QUEUE_COMPLETED = "completed";
export const QUEUE_FAILED = "failed";

export const FINISHED = "finished";

export const METRIC_WORKFLOWS_JOBS_CREATED = "moleculer.workflows.jobs.created";
export const METRIC_WORKFLOWS_JOBS_TOTAL = "moleculer.workflows.jobs.total";
export const METRIC_WORKFLOWS_JOBS_ACTIVE = "moleculer.workflows.jobs.active";
export const METRIC_WORKFLOWS_JOBS_TIME = "moleculer.workflows.jobs.time";
export const METRIC_WORKFLOWS_JOBS_ERRORS_TOTAL = "moleculer.workflows.jobs.errors.total";
export const METRIC_WORKFLOWS_JOBS_RETRIES_TOTAL = "moleculer.workflows.jobs.retries.total";
export const METRIC_WORKFLOWS_SIGNALS_TOTAL = "moleculer.workflows.signals.total";
