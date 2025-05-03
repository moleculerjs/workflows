module.exports = {
	QUEUE_JOB: "job",
	QUEUE_JOB_LOCK: "job-lock",
	QUEUE_MAINTENANCE_LOCK: "maintenance-lock",
	QUEUE_EVENTS: "job-events",
	QUEUE_SIGNAL: "signal",
	QUEUE_WAITING: "waiting",
	QUEUE_ACTIVE: "active",
	QUEUE_DELAYED: "delayed",
	QUEUE_COMPLETED: "completed",
	QUEUE_FAILED: "failed",

	METRIC_WORKFLOWS_RUN_TOTAL: "moleculer.workflows.run.total",
	METRIC_WORKFLOWS_SIGNAL_TOTAL: "moleculer.workflows.signal.total",
	METRIC_WORKFLOWS_EXECUTIONS_TOTAL: "moleculer.workflows.executions.total",
	METRIC_WORKFLOWS_EXECUTIONS_ACTIVE: "moleculer.workflows.executions.active",
	METRIC_WORKFLOWS_EXECUTIONS_TIME: "moleculer.workflows.executions.time",

	METRIC_WORKFLOWS_EXECUTIONS_ERRORS_TOTAL: "moleculer.workflows.executions.errors.total",
	METRIC_WORKFLOWS_EXECUTIONS_RETRIES_TOTAL: "moleculer.workflows.executions.retries.total"
};
