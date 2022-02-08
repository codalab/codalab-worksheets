# Checkpoints

!!! warning "Unreleased feature"
        This feature is still in development. It does not fully work yet and will only work once [#3710](https://github.com/codalab/codalab-worksheets/issues/3710) is resolved.

CodaLab supports the concept of bundle checkpoints, which allow bundles to be stopped and then resumed on different workers. This is especially useful if you want to use preemptible compute nodes, such as [EC2 Spot](https://aws.amazon.com/ec2/spot/) instances. When a worker is preempted, the bundle should be resumed on another worker.

## Workflow steps

This section describes a sample workflow for running bundles on a pool of preemptible workers. The workflow uses the tag `preemptiblepool1` to organize compute, but you can change this tag name as needed.

1. Start a worker manager that runs workers on preemptible compute notes, using the following flags: `--worker-preemptible --worker-tag-exclusive --worker-tag="preemptiblepool1"`. Ensure that all the workers started by the worker manager are configured to share the same network disk for their bundle run working directory / dependency cache.
1. User can run a bundle by running `cl run --tag="preemptiblepool1"`.
1. The bundle manager will assign the bundle to one of the preemptible workers with tag `preemptiblepool1`. If the worker gets preempted, the bundle transitions back to the `STAGED` state and will then be assigned to another preemptible worker with tag `preemptiblepool1`. The history of workers where a bundle has run is stored in the `remote_history` bundle metadata field.
