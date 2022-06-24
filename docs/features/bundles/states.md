## Bundle State Diagram

<br />

![bundle-states](../../images/bundle-states.png)

<br />

## Bundle State Descriptions

| STATE | DESCRIPTION |
| - | - |
| **uploading** | Bundle upload in progress. |
| **created** | Bundle has been created. |
| **staged** | Bundle dependencies are met. |
| **making** | Make bundle is being created. |
| **starting** | Waiting for a worker to start running the bundle. |
| **preparing** | Waiting for the worker to download dependencies and docker images. |
| **running** | Bundle is running. |
| **finalizing** |  Bundle run finished and finalized server-side. The worker will discard it. |
| **ready** | Bundle is done running and has succeeded. |
| **failed** | Bundle is done running and has failed. |
| **killed** | Bundle run was killed manually. |
| **worker_offline** | Worker is temporarily offline. Bundle has not been processed. |
