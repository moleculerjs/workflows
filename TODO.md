# TODO

- [x] Scheduling
  - [x] Cron
  - [x] endDate
- [ ] Job error handling (if it crashed the job, don't save to events, to avoid replaying crasher error)
- [ ] Get workflow state
- [ ] job timeout handling
- [ ] Integration tests
- [ ] Performance improvement
- [ ] Metrics
- [ ] Tracing
- [ ] maxStalledCount - limit the number of putting back the stalled job to wait. (0 means, never, null means everytime)
- [ ] Events:
  - [ ] - `jobCreated`
  - [ ] - `jobStarted`
  - [ ] - `jobStalled`
  - [ ] - `jobCompleted`
  - [ ] - `jobFailed`

