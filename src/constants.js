const SIGNAL_EMPTY_KEY = "null";

const JOB_FIELDS_JSON = ["payload", "repeat", "result", "error", "state"];

const JOB_FIELDS_NUMERIC = [
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

const JOB_FIELDS_BOOLEAN = ["success"];
// const JOB_FIELDS_STRING = ["id", "parent", "nodeID"];
// const JOB_FIELDS = [...JOB_FIELDS_STRING, ...JOB_FIELDS_JSON, ...JOB_FIELDS_NUMERIC, ...JOB_FIELDS_BOOLEAN];

const RERUN_REMOVABLE_FIELDS = [
	"startedAt",
	"duration",
	"finishedAt",
	"nodeID",
	"success",
	"error",
	"result"
];

module.exports = {
	QUEUE_CATEGORY_WF: "workflows",
	QUEUE_CATEGORY_SIGNAL: "signals",
	QUEUE_JOB: "job",
	QUEUE_JOB_LOCK: "job-lock",
	QUEUE_JOB_EVENTS: "job-events",
	QUEUE_MAINTENANCE_LOCK: "maintenance-lock",
	QUEUE_MAINTENANCE_LOCK_DELAYED: "maintenance-lock-delayed",
	QUEUE_WAITING: "waiting",
	QUEUE_ACTIVE: "active",
	QUEUE_STALLED: "stalled",
	QUEUE_DELAYED: "delayed",
	QUEUE_COMPLETED: "completed",
	QUEUE_FAILED: "failed",

	FINISHED: "finished",

	SIGNAL_EMPTY_KEY,
	JOB_FIELDS_JSON,
	JOB_FIELDS_NUMERIC,
	RERUN_REMOVABLE_FIELDS,
	JOB_FIELDS_BOOLEAN,

	METRIC_WORKFLOWS_JOBS_CREATED: "moleculer.workflows.jobs.created",
	METRIC_WORKFLOWS_JOBS_TOTAL: "moleculer.workflows.jobs.total",
	METRIC_WORKFLOWS_JOBS_ACTIVE: "moleculer.workflows.jobs.active",
	METRIC_WORKFLOWS_JOBS_TIME: "moleculer.workflows.jobs.time",
	METRIC_WORKFLOWS_JOBS_ERRORS_TOTAL: "moleculer.workflows.jobs.errors.total",
	METRIC_WORKFLOWS_JOBS_RETRIES_TOTAL: "moleculer.workflows.jobs.retries.total",

	METRIC_WORKFLOWS_SIGNALS_TOTAL: "moleculer.workflows.signals.total"
};
