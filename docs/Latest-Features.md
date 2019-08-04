## Starting version 0.3.0 (March 2019) please refer to our [GitHub Releases](https://github.com/codalab/codalab-worksheets/releases) for latest features

***

## Version 0.2.37 (October 23 2018)
- Minor bugfixes to server

## Version 0.2.36 (October 10 2018)
- Make files open in browser instead of download.
- Now can clone repo to Windows machines.
- New CodaLab default docker images
- Worker reliability improvements.

## Version 0.2.35 (September 20 2018)
- Disk and time quotas for run bundles that do not specify them are now set to the total left for the user. Note that if a user starts multiple runs without specifying these quotas and together the runs go over the quota, one of them will fail
- The CLI may now be configured to pass extra HTTP headers with its REST requests. These may be set up in the CodaLab config file.
- Docker image management fixes: Docker image size limiting process now doesn't touch images actively being used (or expected) by a run. This prevents infinite looping docker image downloads even in pathological cases
- Dependency manager is now more resilient to external filesystem changes and just overwrites if a dependency directory it expects to be empty happens to be there.
- Dependency manager now prunes failed dependencies and retries downloading them if they are requested >10 seconds after the initial failure. This prevents dependency download failures.

## Version 0.2.34 (August 20 2018)
- When the docker container for a run cannot start for a reason (like too little memory requested), the run fails instead of retrying forever
- Rewritten concurrency model in Dependency Manager fixes #929, dependency download speeds should be back to normal. All versions >v0.2.26  and <v0.2.34 are affected from this bug, so if you have any of these versions, please update your worker to 0.2.34
## Version 0.2.33 (August 14 2018)
- Workers now have a `--exit-when-idle` flag which kills the worker process if it is ever idle after checking in with the server. This is part of an ongoing effort to allow ephemeral/one-use workers in shared computation environments like Slurm-managed clusters.
- Fixed a bug where consecutive Markdown blocks were not merged, leading to rendering issues
- Fixed worker dependency manager initialization so the worker can correctly pick up existing cached dependencies upon restart
- Fixed a bug in worker dependency manager state saving that prevented any dependency from getting cleaned up, eventually making the dependency manager hit the disk limit and hang all dependency downloads.
- Fixed a bug where if the Docker container for a run cannot be started, the worker would forever keep attempting instead of failing the bundle. The bundles are failed immediately now if their Docker containers cannot be started.
- Fixed a bug that failed worker check-ins if the check-in included any run that had since been deleted server-side. Now the deleted run is ignored.

## Version 0.2.32 (August 7, 2018)
This version has mainly new worker focused bugfixes as well as a couple tweaks to the CLI
Server Bugfixes & Tweaks
- Fails bundles with killed dependencies unless `--allow-failed-dependencies` specified to fix #930 
- Moves bundles in FINALIZING state but that are not in `worker_run` to `worker_offline` state to fix #930 

Worker Bugfixes & Tweaks
- Fixed a KeyError in LocalFileSystemDependencyManager so Workers can clean up their dependencies successfully.
- Removed the `Touched dependency` log line from LocalFileSystemDependencyManager to make logs more readable and compact

CLI Bugfixes and tweaks
- Add a missing default value to the `fields` argument of `print_user_info` to fix #928 

Version 0.2.31 (August 2, 2018)
- A breaking change that made CLI <= v0.2.26 incompatible with the new server when trying to upload was fixed. As of v0.2.31 CLI <= v0.2.26 should still work fine for uploads.
- Alleviation to an existing bug where trying to include bundle tags in a table schema would break the table rendering.
- Fix to a bug that made dashboard worksheets broken
- Update PyPi dependencies so that `cl install codalab` leads to a full setup again.

If you are on v0.2.27, please update to v0.2.31 as v0.2.27 has these bugs that make it incompatible with previous or later versions.

## Version 0.2.27 (August 2, 2018)
- `.shared` and `group=<group spec>` directives for bundle searches. `.shared` lists any (bundle|worksheet) that any of the groups you are a member of has READ or higher permissions, while `group=<group spec>` lists all (bundles|worksheets) that the given group has READ or higher permissions to.
- Support for the new `worksheet//bundle` style bundle specs when issuing macros or mimic commands. For backwards compatibility we accept both the `worksheet/bundle` and `worksheet//bundle` forms for mimic and macro commands.
- A big rewrite of our Worker code means job state management will be much more robust now, and any failures much easier to debug.
- A major overhaul to worksheet rendering code to make future extensions to the front end easier.
- The new worker code only works with the new server code (due to API changes). If you're running your own CodaLab instance, please make sure to update both your workers and server to version 0.2.27 to avoid failing workers. (If you have workers connected to the public CodaLab instance, please update them)
- The format of the worker state files have changed. If you're updating your worker, please delete the old `dependencies` directory and `dependencies-state.json`, `worker-state.json` and `images-state.json` files.

## Version 0.2.26 (May 19, 2018)
- New admin command "workers" to retrieve info about workers. 
- New admin command "ufarewell" to permanently delete a user. 
- Added fields printing for "uinfo" command.
- Enabled usage of command "cat" in the web command line interface, albeit with a limit of 100 lines.
- Added ".shared" and "group=<group_spec>" args to the worksheet search command, which allows you to find worksheets that are shared you through any group, or a specific group, respectively. 
- Added a more descriptive error message when bundle content path is invalid.
- A request for 0 cpus or less will be bumped up to a request for 1 cpu. 
- The owner of a group cannot be removed as an admin.
- Fixed autocomplete for some functions that used TargetsCompleter.
- Fixed root user creation for clean install of server. 

## Version 0.2.24 (March 10, 2018)
- Macros can now take named optional arguments alongside positional arguments
- Global namespacing for bundles: bundles can now be referred to with `host-worksheet//bundle-name` from other worksheets
- Resource management fixes: GPU, CPU and memory requests now act as exclusive request guarantees, and the limits are enforced properly. Removes the free-for-all memory workers
- URL and Git based bundle uploads can now be made from the Web CLI (`cl up https://example.com/test.txt`)
- `--allow-failed-dependencies` now also allows `killed` dependencies
- Fixes and improvements around worker job management: jobs are resumed better after worker restarts, and long running jobs heartbeat the server, preventing unnecessary `worker_offline` states, offline runs are removed from worker_run
- Bugfix: content bundle uploads `cl upload -c <bundle-contents>` now respect their name argument if given
- Bugfix: content-disposition headers now include `attachment` as previously they were in violation of the RFC

## Version 0.2.23 (Feb 9, 2018)
- Initialize worker docker client ssl context to None by default
- Improved newline-adding logic by cl mimic
- cl upload now ignores files like .DS_Store by default
- Migrated codalab-cli repo to Travis CI
- Run bundles now override container entrypoint regardless of docker image
- Improvements to run bundle scheduler
- Renamed 'cl edit-image' to 'cl docker'
- Added bz2 decompression support
- BETA FEATURE: Interactivity - talk to running bundles via ports

## Version 0.2.22 (Jan 19, 2018)
- GPU workers no longer spin up containers to determine GPU status periodically
- Fixed a security exploit

## Version 0.2.21 (Dec 20, 2017)
- Use docker library to allow authentication
- Slightly better resource allocation
- Bug fixes

## Version 0.2.19 (Nov 1, 2017)
- Made Upload / New Run button functionality dependent on user permissions
- Changed Context Menu (right-clicking on a table item) to depend on user permissions

## Version 0.2.16 (Oct 17, 2017)
- Fixed a bug involving display content with bad paths
- Fixed hanging CLI tests
- Fixed a security exploit

## Version 0.2.15 (Oct 4, 2017)
- Bug fixes

## Version 0.2.13 (June 17, 2017)
- Adds support for anonymous bundles and worksheets
- Moved contents of `worker` directory into `worker/codalabworker`. Make `worker` its own package with a new `setup.py`.
- Various bugfixes

## Version 0.2.7 (May 18, 2017)
- Support multiple comma-separated list of hosts for Torque scheduler (e.g. `--request-queue host=john7,john11`)
- Various bugfixes

## Version 0.2.6 (May 13, 2017)
- Improved compression API ([cli#732](https://github.com/codalab/codalab-cli/pull/732))
- Improve torque scheduler ([cli#743](https://github.com/codalab/codalab-cli/pull/743))
- Implements human readable fields in content API ([cli#748](https://github.com/codalab/codalab-cli/pull/748))

## Version 0.2.5 (April 25, 2017)
- Improve help box user interface
- Bugfix: Slashes in bundle content links no longer escaped ([worksheets#298](https://github.com/codalab/codalab-worksheets/pull/298))

## Version 0.2.4 (April 9, 2017)
- New algorithm for load-balancing bundle storage ([cli#711](https://github.com/codalab/codalab-cli/pull/711))
- Script for maintaining a competition leaderboard: see `codalab-cli/scripts/competitiond.py`
- Bugfixes: [cli#719](https://github.com/codalab/codalab-cli/issues/719), [cli#696](https://github.com/codalab/codalab-cli/issues/696), [cli#700](https://github.com/codalab/codalab-cli/issues/700)

## Version 0.2.3 (March 11, 2017)
- New commands for building and testing Docker images: `cl edit-image`, `cl commit-image`, `cl push-image`, and `cl run --local`.
- Improved support for GPU workers

## Version 0.2.2 (February 24, 2016)
- CLI now available through `pip install codalab-cli`!
- New help button on the website to get support faster.
- You can now set bundle and worksheet permissions for groups that you don't belong to ([cli#671](https://github.com/codalab/codalab-cli/issues/671))
- Support more Docker images ([cli#224](https://github.com/codalab/codalab-cli/issues/224))
- Bug fixes for [cli#537](https://github.com/codalab/codalab-cli/issues/537), [cli#625](https://github.com/codalab/codalab-cli/issues/625), [worksheets#242](https://github.com/codalab/codalab-worksheets/issues/242), [worksheets#222](https://github.com/codalab/codalab-worksheets/issues/222), [worksheets#215](https://github.com/codalab/codalab-worksheets/issues/215), [worksheets#244](https://github.com/codalab/codalab-worksheets/issues/244)

## Version 0.2.1 (January 25, 2016)
- Integrated worker with [nvidia-docker](https://github.com/NVIDIA/nvidia-docker) to support GPU jobs
- Bug fixes for [cli#621](https://github.com/codalab/codalab-cli/issues/621), [cli#624](https://github.com/codalab/codalab-cli/issues/624), [cli#627](https://github.com/codalab/codalab-cli/issues/627), [cli#619](https://github.com/codalab/codalab-cli/issues/619), [cli#591](https://github.com/codalab/codalab-cli/issues/591), [worksheets#235](https://github.com/codalab/codalab-worksheets/issues/235)

## Version 0.2.0 (November 7, 2016) 
- **Backwards-incompatible changes, please upgrade your CLI with `git pull`!**
- Migrated last remaining APIs to the new REST API and retired the old XML-RPC API.
  - _CodaLab server administrators_: you no longer need to run `cl rest-server`.
  - [Public documentation on the API](REST-API-Reference.md) will be made available soon.
- Removed remaining support for the connecting to `local` on the CLI. If you want to do CodaLab work entirely on your machine, you must [set up your own server processes](Server-Setup.md).
- Faster upload through both the website ([#196](https://github.com/codalab/codalab-worksheets/issues/196)) and the CLI ([#599](https://github.com/codalab/codalab-cli/issues/599)).
- CLI notifies user when a new version is available.
- Bug fixes for [worksheets#216](https://github.com/codalab/codalab-worksheets/issues/216), [worksheets#211](https://github.com/codalab/codalab-worksheets/issues/211), [worksheets#164](https://github.com/codalab/codalab-worksheets/issues/164), [cli#326](https://github.com/codalab/codalab-cli/issues/326), [cli#575](https://github.com/codalab/codalab-cli/issues/575)

## Version 0.1.11 (September 3, 2016)
- Automatically notify admins by email when unexpected errors occur
- Automatically refresh unfinished bundles (not 'ready' or 'failed')
- Bug fixes: run bundle button only loads bundles when they are drilled down

## Version 0.1.10 (August 7, 2016)
- Add bundle contents API, with improvements to upload/download speed from the CLI
- Allow addition of tags to bundles with `cl edit --tags`
- New Docker image for Torch
- Add colors to setup script
- Bug fixes: login issues, public worksheets, `uedit` disk command

## Version 0.1.9 (July 9, 2016)
- Simplify worksheets page, layout of the metadata, and default bundle fields.
- Consolidate setup/update into `./setup.sh` and simplify deployment process.
- Fix monitor script.

## Version 0.1.8 (May 19, 2016)
 - Add ability to change passwords.
 - New worker system.
 - Fix copy and paste errors on Mac OS.
 - Speed-up loading of a worksheet

## Version 0.1.7 (Apr. 21, 2016)
 - Bring back the My Account page and password reset link removed during the user table migration.
 - Refactoring of upload logic.
 - Log last login
 - Bug fix for private bundles
 - Bug fixes for [worksheets#130](https://github.com/codalab/codalab-worksheets/issues/130), [worksheets#133](https://github.com/codalab/codalab-worksheets/issues/133)

## Version 0.1.6 (Mar. 16, 2016):
 - Migrate user tables from the Django DB to the Bundles DB, get rid of Django DB.
 - Migrate the REST APIs from DJANGO to the new REST server.
 - Refactoring of download logic.
 - New Run button
 - New Worksheet button
 - Chatbox backend
 - More sane handling of symbolic links

## Version 0.1.5 (Mar. 4, 2016):
 - Implement saving bundles to multiple disks
 - Add a script to check bundle store sanity (scripts/sanity-check-bundlestore.py)
 - Add the --allow-failed-dependencies flag to the run command.
 - Bugfixes for [worksheets#103](https://github.com/codalab/codalab-worksheets/issues/103), [cli#345](https://github.com/codalab/codalab-cli/issues/345), 

## Version 0.1.4 (Feb. 22, 2016):
- Bugfixes for [cli#339](https://github.com/codalab/codalab-cli/pull/341), [worksheets#85](https://github.com/codalab/codalab-worksheets/issues/85)

## Version 0.1.3 (Feb. 20, 2016):
- Store bundles in directories named using the UUID, instead of data hash.
- Makeover of front page to make it easier for people to figure out what CodaLab is

## Version 0.1.2 (Feb. 13, 2016):
- Worksheets officially decoupled from Competitions
- Improved performance on worksheets web client with precompiled JSX
- Web terminal now prints usage help
- Disabled autocomplete for `cl` command aliases
- Bugfixes for [cli#287](https://github.com/codalab/codalab-cli/issues/287), [cli#307](https://github.com/codalab/codalab-cli/issues/307), [cli#330](https://github.com/codalab/codalab-cli/pull/330), [worksheets#32](https://github.com/codalab/codalab-worksheets/issues/32), [worksheets#26](https://github.com/codalab/codalab-worksheets/issues/26)


## Version 0.1.1 (Dec. 8, 2015):
- Improved behavior and robustness for editable fields on website
- Better support for long commands on web terminal
- Change worksheets on website without reloading application
- Uploading bundles on website no longer uses modal

## Version 0.1.0:
- Support for user time/disk quotas (`cl status` to show quotas and `cl uedit` to change if you're an admin)
- Support for writing to files inside a running bundle (usage: `cl write <target> "string to write"`)
- Support for graphing (usage: `% display graph <path-to-tsv-file> xlabel=... ylabel=...`)
- Switched uploading/downloading of bundles to use .tar.gz instead of .zip so that they can be streamed.
- Much nicer worksheet editing interface with syntax highlighting and keeping tracking of the current location.
- Added support for executing Jupyter/IPython notebooks.
- Upload directly from GitHub (`cl upload https://github.com/percyliang/fig --git`)
- Press `u` and `a` to paste uuid and arguments of current bundle into the web terminal, to make it easy to re-execute jobs