# TODO

- [ ] retries -> retryPolicy based on index.d.ts
  - [ ] maxDelay
  - [ ] exponentialFactor: 2

- [x] Better processing delayed/failed job (without waiting for maintenance time)
  - [x] After the jobId puts into the delayed queue, check the head/first eelment with ZRANGE. If the head is this jobId, we should notify allworkers about new maintenance time, publish a msg to workers
- [ ] Maintenance execution optimization (every worker start a 10 sec timer and execute the maintenance jobs)

- [ ] Integration tests
  - [ ] different serializer
  - [ ] Retries (ctx.mcall)
  - [ ] middleware hooks
  - [ ] disabled workflow
  - [ ] limiting maintenance execution at multiple workers
  - [ ] Delayed job processing at the promotion time
  
- [ ] Metrics
- [ ] Tracing

- [ ] Performance improvement

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

