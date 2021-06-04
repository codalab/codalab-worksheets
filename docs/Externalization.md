# Storage Externalization

## Link functionality

CodaLab allows storage externalization by passing a file path to the `--link` argument when running `cl upload`. This makes the path the source of truth of the bundle, meaning that the server will retrieve the bundle directly from the specified path rather than storing its contents in its own bundle store.

Note that the path specified by `--link` must be accessible by the CodaLab server, so the `--link` functionality is useful for private instances of CodaLab in which the server and users both have access to the same filesystem.

In this case, it would be inefficient to copy every bundle upload to a separate bundle store. Additionally, using `--link` would make debugging easier by allowing users to directly mutate the underlying files that a bundle is associated with.

!!! note
    The `--link` functionality is not enabled by default on CodaLab installations. Read [Server setup](#server-setup) for more information on how to enable it.

### CLI usage

Here is an example of using the link functionality:

```bash
cl upload /u/nlp/data/a.txt --link
```

In the above example, the upload command happens instantaneously (with no data transfer of the actual file), and the server will retrieve the data from the given link path when the bundle is required as a dependency.


Compare this operation to a normal upload, in which `/u/nlp/data/a.txt` will be uploaded to the server and a copy of it will be stored in the bundle store:

```bash
cl upload /u/nlp/data/a.txt
```

### Server setup

In order to use `--link` with an existing filesystem, set either the `CODALAB_LINK_MOUNTS` environment variable or the `--link_mounts` command-line parameter to `codalab_service.py` on the server.

This variable must be equal to a file path on the server that will be mounted onto the CodaLab server Docker container so it can access them. To specify multiple file paths to be mounted, separate these file paths with a comma.

For example, in order to restrict all possible linked paths to files inside the `/u/nlp/data` directory or `/u/nlp/output` directory, use:

```bash
CODALAB_LINK_MOUNTS=/u/nlp/data,/u/nlp/output python codalab_service.py
```

If this argument is not specified, the `/tmp/codalab/link-mounts` directory will be mounted by default.

### Design

#### Mounting process

The `--link_mounts` parameter is necessary because the CodaLab server runs on a Docker container and does not have access to the entire host's filesystem
by default.

Each path that is passed to `--link_mounts` will be mounted in a directory called `/opt/codalab-worksheets-link-mounts` on the Docker container, at a location that mirrors the original path in the host filesystem.

For example, if `--link_mounts=/u/nlp/data` is specified, this means that the host path `/u/nlp/data` will be mounted to `/opt/codalab-worksheets-link-mounts/u/nlp/data` on the CodaLab server Docker container.


#### The interplay between --link and --shared-file-system

It is important to note how `--link` differs from `--shared-file-system`:

- `--link` is an argument passed to
`cl upload` that means that the user is referencing a path that is already on the server (which typically means that the
user and the server share a file system).
- `--shared-file-system` is an argument passed to `codalab_service.py` that
means that the _workers_ and the server share a file system from which they can retrieve bundle contents.

When a bundle is run with a dependency, here is what the CodaLab server sends the worker, based on different combinations
of `--link` (applied to the dependency) and `--shared-file-system`:

---       |--link | no --link            |
-------| ----------|------------------------------|
 --shared-file-system        | Server sends worker just the bundle path (`/u/nlp/data`) | Server sends worker just the bundle path (which must be in CodaLab bundle store `/u/codalab/bundles/...`)
 no --shared-file-system     | Server sends worker the bundle contents (reading from the path - `/u/nlp/data`)   | Server sends worker the bundle contents (reading from the path in the CodaLab bundle store - `/u/codalab/bundles/...`)

## Azure Blob Storage

Azure Blob Storage can be used as a bundle store. Note that this feature is in beta and may change.

This feature is **in progress.** Currently, bundles can only be uploaded to Blob Storage, but bundles cannot be downloaded!

To upload a file with Azure Blob Storage, use `cl upload` with the `--use-azure-blob-storage-beta` option (or `-a`):

```bash
cl upload test.txt -a
```

### Server setup

In order to enable Azure Blob Storage as a bundle store, follow these instructions:

1. Create an Azure Blob Storage Account.
1. Create a container named "bundles" from the Azure Portal.
1. Set the `CODALAB_AZURE_BLOB_CONNECTION_STRING` environment variable to the Blob Storage connection string before starting the server.

If you want to force the rest server to always use Azure Blob Storage in the upload endpoint, set the `CODALAB_ALWAYS_USE_AZURE_BLOB_BETA` environment variable there:

```bash
CODALAB_AZURE_BLOB_CONNECTION_STRING=... CODALAB_ALWAYS_USE_AZURE_BLOB_BETA=1 cls start -bd
```

### Local development

During local development, you can simulate the Azure Blob Storage Account by running the `azurite` service from `codalab_service.py`. By default, this service is not run, so you must explicitly specify it:

```
python codalab_service.py start -bds default azurite
```
