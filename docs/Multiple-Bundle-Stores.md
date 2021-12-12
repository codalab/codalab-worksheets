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

## Steps to set up GCP

```
cl store add --name gcp --storage-type gcp --url gcs://bucket1
cl upload --store gcp mkdocs.yml
```