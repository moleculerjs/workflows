# TODO

- [x] Scheduling
  - [x] Cron
  - [x] endDate
  - [x] count of executions
  - [x] Remove repeated job by jobId

- [x] Job error handling (if it crashed the job, don't save to events, to avoid replaying crasher error)
- [x] Get workflow state
- [ ] 
- [ ] jobId collision policy
  - [ ] reject
  - [ ] skip
  - [ ] rerun

- [ ] job timeout handling
- [ ] Signal wait timeout `WfSignalTimeoutError`

- [ ] wait for result `const result = await broker.wf.run(...).promise()`
  - [ ] job will run on other nodes, so it should subscribe for `job.finished` Redis msg and resolve or reject the stored promise.

- [ ] Better processing delayed/failed job (without waiting for maintenance time)
- [ ] Concurrent job running
- [ ] Job parameter validation
- [x] maxStalledCount - limit the number of putting back the stalled job to wait. (0 means, never, null means everytime)

- [ ] Moleculer Events:
  - [x] `jobEventType: "broadcast","emit", null - disable
  - [x] - `job.${workflowName}.created`
  - [x] - `job.${workflowName}.started`
  - [x] - `job.${workflowName}.stalled`
  - [x] - `job.${workflowName}.finished`
  - [x] - `job.${workflowName}.completed`
  - [x] - `job.${workflowName}.failed`

```js

```

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

- [ ] Performance improvement
- [ ] Integration tests
- [ ] Metrics
- [ ] Tracing
