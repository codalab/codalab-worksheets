## Bundle State Descriptions

There are three different bundle types: uploaded bundles, make bundles and run
bundles. Each bundle type will enter various states while it is being processed.
Below are the various bundle state types and definitions:

<br />

### Uploaded Bundles

| State | Name | Description |
| - | - | - |
| 0 | **created** | Bundle has been created but its contents have not been uploaded yet. |
| 1 | **uploading** | Bundle contents are being uploaded. |
| 2a | **ready** | Bundle has completed uploading. |
| 2b | **failed** | Bundle has failed. |

<br />

### Make Bundles

| State | Name | Description |
| - | - | - |
| 0 | **created** | Bundle has been created but its contents have not been populated yet. |
| 1 | **making** | Bundle contents are being populated by copying its dependencies. |
| 2a | **ready** | Bundle contents have been populated. |
| 2b | **failed** | Bundle has failed. |

<br />

### Run Bundles

| State | Name | Description |
| - | - | - |
| 0 | **created** | Bundle has been created but its contents have not been populated yet. |
| 1 | **staged** | Bundle’s dependencies are all ready. Just waiting for workers to do their job. |
| 2 | **starting** | Bundle has been assigned to a worker and waiting for worker to start the bundle. |
| 3 | **preparing** | Waiting for worker to download dependencies and container image to run the bundle. |
| 4 | **running** | Bundle command is being executed in a container. Results are uploading. |
| 5 | **finalizing** | Bundle command has finished executing, deleting from worker. |
| 6a | **ready** | Bundle command is finished executing successfully, and results have been uploaded to the server. |
| 6b | **failed** | Bundle has failed. |
| 6c | **killed** | Bundle was killed by the user, and results have been uploaded to the server. |
| Offline | **worker_offline** | The worker where the bundle is running on is offline. |
