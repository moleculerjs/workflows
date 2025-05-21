# TODO

- [ ] Abort execution, put to a sleep queue
  - [ ] if a ctx.wf.sleep is longer than 5 minutes, put it to a sleep queue (mwOpts)
  - [ ] if a waitForSignal waits longer than 1 minute (mwOpts)

- [ ] **Workflow class**
  - [ ] Create a workflow class which contains the workflow logic (move from Redis & Base adapter classes)
  - [ ] Start the workflow when parsing the service schema
  - [ ] Stop the workflow when the service is stopped
  - [ ] Workflow creates an adapter instance and handle it directly (no central adapter)

- [ ] SAGA
  - [ ] compensations
  - [ ] revert running

- [ ] Metrics
- [ ] Tracing

- [ ] Performance improvement
