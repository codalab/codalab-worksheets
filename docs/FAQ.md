### I installed CodaLab using `pip`. Why am I seeing `-bash: cl: command not found` when trying to use the CLI?

When installing CodaLab, `pip` will output the following error if the executable path is not in your $PATH. 

    WARNING: The scripts cl, cl-bundle-manager, cl-competitiond, cl-server, cl-worker, cl-worker-manager and codalab-service are installed in '<cl path>' which is not on PATH.
    Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.

For Mac and Linux users, you can resolve this by appending `export PATH="$PATH:<cl path>"` to your `~/.bashrc`.

For Windows users, select `System` from the `Control Panel`, select `Advance System Settings`, go to `Environment Variables` and add the path to the list.

### Why is my run bundle in `staged` for a long time?

The `staged` state means that the bundle is ready to run but it is waiting for appropriate resources to free up.  CodaLab has a limited set of machines available (we're offering it for free after all), so when there are a lot of people wanting to run jobs, you might have to wait a long time.  You can check out the [CodaLab status](https://worksheets.codalab.org/worksheets/0xa590fd1b68944a1a95c1c40c4931dc7b/) page to see where your jobs are in the queue (note that this page only sees public jobs, so there might be hidden bundles in the queue that you can't see).  If you'd like to run your job earlier, you can easily [attach your own compute workers](Execution.md#running-your-own-worker), which you might want to do anyway if you have fancier GPUs, say.

### What does it mean for my run bundle to be in the `worker_offline` state?

This means that the worker that was running your job is unreachable.  This might mean that the worker crashed or that this is just a temporary state and the worker will come back.  If your bundle persists in this state for a few hours, then your run will probably unlikely come back.  In this case, you can try re-starting your job.  If your job continues to end up in the `worker_offline` state, then something must be wrong, and please contact us for assistance.

### How do I run my run bundle on a GPU?

See our [article on running jobs that use GPUs](Execution.md#running-jobs-that-use-gpus).

### How do I share a bundle only with other CodaLab users?

By default, all bundles (and worksheets) on CodaLab are public (and we encourage this).  But if you need to make your bundle private to you and your team, check out the instructions [here](CLI-Reference.md#permissions).

### I received an `ImportError` because I don't have the appropriate library installed. How do I install libraries in CodaLab?

CodaLab runs all run bundles in Docker containers. See our [article about specifying Docker containers](Execution.md#specifying-environments-with-docker).

### How do I check how much space my bundles are using?

You can use `cl uinfo` to find out how much disk space you have left.  To find out which bundles are taking the most space, you can check "My Dashboard" or type `cl search .mine size=.sort-`.

### How do I reduce the amount of disk usage?

If you have used up your disk quota and want to reduce your disk usage, here are a few tips:
- Make sure that you don't have unused bundles lying around.  Type `cl search .mine size=.sort-` to find the largest bundles.  Note that some of these bundles might be floating (not showing up on any worksheet).
- If you have intermediate bundles whose contents you don't need, you can use `cl rm -d <bundle>` to delete only the contents of the bundle but keep the metadata.  This way, you keep the provenance and can regenerate this bundle in the future.
- If there are selected files that you wish to keep in a bundle, you can use `cl make <bundle>/<important-file>` to make a new bundle that only has a copy of the desired file and then you can do `cl rm -d <bundle>` to remove all the other files in that bundle.

### How do I request more disk quota?

If you have tried the steps above and still need more disk quota, please fill out this [form](https://docs.google.com/forms/d/e/1FAIpQLSdS22MaLyE1kx0JL-w2a8f8IasDyc36H_ZuidNBVAE_afMCpw/viewform).

### How do I upload large bundles?

Using `cl upload` is your best bet.  If you can't use the CLI (e.g., because you're behind a proxy), you can use the web interface (for directories, upload a zip file).  If this fails, then you can put your bundle at a public URL, and then use `cl upload http://...` from the web terminal in the web interface to "upload" that location to CodaLab.

### Adding bundles to a worksheet via `cl add <bundle> .` doesn't work.

This is the old syntax.  Do `cl add <bundle>` to add to the current worksheet and `cl add <bundle> -w <worksheet>` to add to a particular worksheet.

### I upgraded CodaLab and now I'm getting an error like: `ValueError: unknown url type: local/rest/worksheets?specs=%2F`

Run `cl alias main https://worksheets.codalab.org` and retry your command.

### When I try to execute a command like make on CodaLab, it fails because of "read-only" permissions. What do I do?

Each bundle uploaded to CodaLab is read-only. If a command you are running creates new files, you'll have to create a duplicate of the in the run command. For example, instead of running `cl run :src 'cd src && make`, run `cl run src-orig:src 'cp -RL src-orig src && cd src && make'`. Your freshly compiled executable will now be present in the `src` directory inside the last bundle.

### How much resources (memory, GPUs, etc.) does my run get?

The exact resources vary at any point in time, but you can do `cl run "free; nvidia-smi; df"` to get the most accurate statistics on the actual environment.  Remember to use `--request-memory`, `--request-gpus` appropriately.

### How do I restrict a search to only bundles in a specific worksheet?

Run `cl search host_worksheet=<worksheet>`.  A common mistake is to do `cl
search -w <worksheet>`, which will only run the search in the context of the
given worksheet (which is consistent with other `cl` commands).
