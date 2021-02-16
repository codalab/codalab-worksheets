CodaLab is centered around the idea of **bundles**, which can store data. You can upload a file / folder to CodaLab to create a bundle, and then you can download the entire bundle or parts of the bundle.

In CodaLab, you can upload bundles to the bundle store. To do so, you can either use the `cl upload` command from the CLI or click on the "Upload" button from the worksheets interface.

## Using the CLI

You can upload bundles from the CLI with the `cl upload` command.

Let's upload a folder from the CLI:

``` bash
% mkdir -p myfolder
% echo "hello world" > myfolder/one.md
% echo "goodbye world" > myfolder/two.md
% cl upload myfolder
Preparing upload archive...
Uploading myfolder.tar.gz (0xdc74e4ef29b64ab19e1e26e94ea22811) to https://worksheets-dev.codalab.org
Sent 0.01MiB [0.07MiB/sec]    
0xdc74e4ef29b64ab19e1e26e94ea22811
```

Now, one can also view the information of the uploaded bundle using the `cl info` command:

``` bash
% cl info 0xdc74e4ef29b64ab19e1e26e94ea22811
bundle_type               : dataset
uuid                      : 0xdc74e4ef29b64ab19e1e26e94ea22811
data_hash                 : 0x28bf5b81a526b5c049d9a3e5e8cdce5541182506
state                     : ready
command                   : <none>
is_anonymous              : False
owner                     : codalab(0)
name                      : myfolder
created                   : 2021-02-16 11:53:56
data_size                 : 4.0k
```

You can preview the bundle, or individual files in it, using `cl cat`:

``` bash
% cl cat 0xdc74e4ef29b64ab19e1e26e94ea22811
name    perm   size
-------------------
one.md  0o644    12
two.md  0o644    14
% cl cat 0xdc74e4ef29b64ab19e1e26e94ea22811/one.md
hello world
```

Finally, you can download the entire bundle, or individual files within it, using `cl download`:

``` bash
% cl download 0xdc74e4ef29b64ab19e1e26e94ea22811 -o output
Downloading myfolder(0xdc74e4ef29b64ab19e1e26e94ea22811)/ => .../output
Received 0.00MiB [0.05MiB/sec] 
% cl download 0xdc74e4ef29b64ab19e1e26e94ea22811/one.md -o output-one.md
Downloading myfolder(0xdc74e4ef29b64ab19e1e26e94ea22811)/one.md => .../output-one.md
Received 0.00MiB [0.00MiB/sec]
```

## Using the web interface

From the web interface, click on the "Upload" button to upload files or folders.

![upload](../../images/upload.png)

Once you select the file / folder to upload, it will be added to the worksheet:

![uploaded](../../images/uploaded.png)

You can click on the bundle to view more information or download it.