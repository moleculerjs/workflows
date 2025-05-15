# TODO

- [ ] **rename repo to "workflows"**

- [ ] job timeout handling
  - [ ] workflow property, maintenance process which gets active jobs and checks the startedAt property. If it's timed out, close as failed.
- [ ] Signal wait timeout `WfSignalTimeoutError`
- [ ] retries -> retryPolicy based on index.d.ts
  - [ ] maxDelay
  - [ ] exponentialFactor: 2

- [ ] Better processing delayed/failed job (without waiting for maintenance time)
  - [ ] After the jobId puts into the delayed queue, check the head/first eelment with ZRANGE. If the head is this jobId, we should notify allworkers about new maintenance time, publish a msg to workers

- [ ] List functions
  - [ ] `listCompletedJobs(workflowName)`
  - [ ] `listFailedJobs(workflowName)`
  - [ ] `listDelayedJobs(workflowName)`
  - [ ] `listActiveJobs(workflowName)`
  - [ ] `listWaitingJobs(workflowName)`

- [ ] Integration tests
  - [ ] different serializer
  - [ ] jobId collision
  - [ ] jobEvents
  - [ ] Retries (ctx.mcall)
  - [ ] job timeout
  - [ ] middleware hooks

- [ ] Metrics
- [ ] Tracing

- [ ] Performance improvement

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

