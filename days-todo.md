# 5/29 TODO:
## Done Tests:
  - Assign max CPUs to a run, stop worker, create another run, bring worker back to life
    - TODO: This currently doesn't work well, the worker accepts the run and throws an exception at each event loop. It should deny it until the resources free up
    - What should happen?
      1. With checkin worker should report its resumed runs (Is this happening? [x])
      2. Server when reading checkin should process the reported runs (update worker_run [ ])
        - Calls resume_bundle on each one
      3. Server should then decide to assign runs or not [ ]


## To be tested:
  - Start more involved runs on dev worker 1
    - Have two runs that require same big dependency
    - Have two runs that require same big docker image
    - Have a run that depends on another run
    - Have runs that go over each resource constraint category:
      - memory
      - time
      - disk use
    - Test killing during dependency download
    - Test killing during docker image download
    - Test killing during uploading results
  - Deploy a dev GPU worker and do more involved runs there
  - Test worker down/up cycles:
    - Kill the worker during a dependency download, restart, dep download should restart
    - Kill the worker during a docker image download, restart, image download should restart
    - Kill the worker after the dep. downloads are done, restart, it should start running
    - Kill the worker during running, it should go back to running
    - Kill the worker during uploading results, it should restart uploading
  - Bring down worker mid run, remove run server side, bring worker back up
  - Start writing some of the unit tests
