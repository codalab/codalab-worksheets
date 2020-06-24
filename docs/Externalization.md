# Storage Externalization

## --link

CodaLab allows storage externalization by using the `--link` argument when running `cl upload`. This makes the path the source of truth of the bundle, meaning that the server will retrieve the bundle directly from the specified path rather than storing its contents in its own bundle store.

Note that the path specified by `--link` must be accessible by the CodaLab server, so the `--link` functionality is useful when you have a shared filesystem that both the server and users of CodaLab access, and the users just want to refer to specific files on the filesystem.

Here is an example of using the link functionality:

```bash
cl upload /var/data/a.txt --link
```

In the above example, the upload command happens instantaneously (with no data transfer of the actual file), and the server will retrieve the data from the given link path when the bundle is required as a dependency.


Compare this operation to a normal upload, in which `/var/data/a.txt` will be uploaded to the server and a copy of it will be stored in the bundle store:

```bash
cl upload /var/data/a.txt
```

### Allowed paths

For security reasons, in order for the `--link` command to work with file paths, either the `CODALAB_LINK_ALLOWED_PATHS` environment variable or the `--link_allowed_paths` command-line parameter to `codalab_service.py` must be set on the server. This must be equal to a glob expression that specifies the allowed paths that `--link` can grab data from.

For example, in order to restrict all linked paths to files inside the `/var/data` directory, run the server with this command:

```bash
python codalab_service.py --link_allowed_paths=/var/data/**
```