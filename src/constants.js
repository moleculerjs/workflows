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

	METRIC_WORKFLOWS_JOBS_CREATED: "moleculer.workflows.jobs.created",
	METRIC_WORKFLOWS_JOBS_TOTAL: "moleculer.workflows.jobs.total",
	METRIC_WORKFLOWS_JOBS_ACTIVE: "moleculer.workflows.jobs.active",
	METRIC_WORKFLOWS_JOBS_TIME: "moleculer.workflows.jobs.time",
	METRIC_WORKFLOWS_JOBS_ERRORS_TOTAL: "moleculer.workflows.jobs.errors.total",
	METRIC_WORKFLOWS_JOBS_RETRIES_TOTAL: "moleculer.workflows.jobs.retries.total",

	METRIC_WORKFLOWS_SIGNAL_TOTAL: "moleculer.workflows.signal.total"
};
