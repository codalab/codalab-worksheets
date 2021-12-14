# Multiple Bundle Stores (in development)

!!! warning "Unreleased feature"
        This feature is still in development. It does not fully work yet and will only work once [#3710](https://github.com/codalab/codalab-worksheets/issues/3710) is resolved.

CodaLab allows you to specify multiple bundle stores.

## Workflow steps

This workflow describes how to set up a bundle store on GCP.

To create a bundle store, run the following:

```bash
cl store add --name store1 --storage-type disk --storage-format uncompressed
```

You can list bundle stores by running

```bash
cl store ls
```

You can also delete a bundle store by running

```bash
cl store rm [bundle store uuid]
```

## Steps to test locally with Azurite

First, make sure you start Azurite locally by running `codalab-service start -bds default azurite`. Then run:

```
cl store add --name blob --storage-type azure_blob --url azfs://devstoreaccount1/bundles
cl upload --store blob mkdocs.yml
```

## Steps to set up and test with GCP

First, make sure the `CODALAB_GOOGLE_APPLICATION_CREDENTIALS` env var is set on the server that runs the REST server. It should be set to a path to a credentials JSON file.

```
export CODALAB_GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
codalab-service start -bd
```

Then make a bucket on your GCP account (in this case, it's called `ashwin123123123`). You can then store bundles on GCP:

```
cl store add --name gcp2 --storage-type gcs --url gs://ashwin123123123
cl upload --store gcp2 mkdocs.yml
```