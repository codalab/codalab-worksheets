# 5/29 TODO:
## Done Tests:
  - Assign max CPUs to a run, stop worker, create another run, bring worker back to life [x]


## Currently testing:
  - Have two runs that require same big dependency
    - Something wrong with dep. manager


## To be tested:
  - Start more involved runs on dev worker 1
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
