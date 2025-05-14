# TODO

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
  - [ ] Repeat jobs
  - [x] Delayed jobs
  - [ ] different serializer
  - [ ] jobId collision
  - [x] jobEvents
  - [ ] Retries
    - [ ] skipping already executed tasks
      - [ ] ctx.call
      - [ ] ctx.emit
      - [ ] ctx.broadcast
      - [ ] ctx.mcall
      - [ ] ctx.wf.task
  - [ ] multi nodes
  - [ ] stalled jobs
  - [ ] lock extending
  - [ ] removeOnFailed
  - [ ] removeOnCompleted
  - [ ] retention
  - [ ] job timeout
  - [x] parameter validation
  - [ ] middleware hooks
- [ ] Metrics
- [ ] Tracing
