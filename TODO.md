# TODO

- [ ] **rename repo to "workflows"**
- [ ] jobId collision policy
  - [ ] reject
  - [ ] skip
  - [ ] rerun

- [ ] job timeout handling
- [ ] Signal wait timeout `WfSignalTimeoutError`
- [ ] retries -> retryPolicy based on index.d.ts

- [ ] Better processing delayed/failed job (without waiting for maintenance time)

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

- [ ] Performance improvement

- [ ] Unit tests
  - [x] getKey
    - [x] prefix
  - [x] adapter resolve

- [ ] Integration tests
  - [x] Repeat jobs
  - [x] Delayed jobs
  - [ ] different serializer
  - [ ] jobId collision
  - [x] jobEvents
  - [x] Retries
    - [x] skipping already executed tasks
      - [x] ctx.call
      - [x] ctx.emit
      - [x] ctx.broadcast
      - [ ] ctx.mcall
      - [x] ctx.wf.task
  - [x] multi nodes
  - [x] stalled jobs
  - [x] lock extending
  - [ ] removeOnFailed
  - [ ] removeOnCompleted
  - [ ] retention
  - [ ] job timeout
  - [x] parameter validation
  - [ ] middleware hooks
- [ ] Metrics
- [ ] Tracing
