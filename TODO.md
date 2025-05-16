# TODO

- [ ] retries -> retryPolicy based on index.d.ts
  - [ ] maxDelay
  - [ ] exponentialFactor: 2

- [ ] Better processing delayed/failed job (without waiting for maintenance time)
  - [ ] After the jobId puts into the delayed queue, check the head/first eelment with ZRANGE. If the head is this jobId, we should notify allworkers about new maintenance time, publish a msg to workers

- [ ] Integration tests
  - [ ] different serializer
  - [ ] Retries (ctx.mcall)
  - [ ] middleware hooks
  
- [ ] Metrics
- [ ] Tracing

- [ ] Performance improvement

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

