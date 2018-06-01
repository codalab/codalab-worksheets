# TODO:
## Done Tests:
  - Assign max CPUs to a run, stop worker, create another run, bring worker back to life
  - DockerImageState objects end up in DependencyManager state queue (bad pyjson default param)
  - Fixed bad state management that didn't clean up dependency symlinks before upload
  - Have a run that depends on another run
  - Dependency symlinking is broken (fixed, dir structure was bad)
  - Test worker down/up cycles:
    - Kill the worker during a dependency download, restart, dep download should restart
    - Kill the worker during a docker image download, restart, image download should restart
    - Kill the worker after the dep downloads are done, restart, it should start running
    - Kill the worker after the docker downloads are done, restart, it should start running
    - Kill the worker during running, it should go back to running
    - Kill the worker during uploading results, it should restart uploading
  - Have two runs that require same big dependency (confirmed succesfully waiting for download together)
  - Have two runs that require same big docker image

## Currently testing:
  - Test a multi-day run

## To be tested:
  - Deploy a dev GPU worker and do more involved runs there
  - Bring down worker mid run, remove run server side, bring worker back up
  - Start writing some of the unit tests
