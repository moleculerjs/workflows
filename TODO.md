# TODO

- [ ] jobId collision policy
  - [ ] reject
  - [ ] skip
  - [ ] rerun

- [ ] job timeout handling
- [ ] Signal wait timeout `WfSignalTimeoutError`
- [ ] retries -> retryPolicy based on index.d.ts

- [x] wait for result `const result = await broker.wf.run(...).promise()`
  - [x] job will run on other nodes, so it should subscribe for `job.finished` Redis msg and resolve or reject the stored promise.

- [ ] Better processing delayed/failed job (without waiting for maintenance time)
- [x] Concurrent job running
- [x] Job parameter validation


- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

- [ ] Performance improvement
- [ ] Integration tests
- [ ] Metrics
- [ ] Tracing
