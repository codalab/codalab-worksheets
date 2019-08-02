# Common Usecases
This wiki document is intended to guide users to perform common tasks with codalab.

## Snapshot an ipython notebook in a worksheet
While ipython notebooks provide a great interactive tool to explore and experiment with data and algorithms, they often require non-trivial package installations. Uploading a notebook on codalab allow any experiments written in ipython to be replicated regardless of platform.

In this use-case, we will look at the worksheet for the executable paper [polymom](https://worksheets.codalab.org/worksheets/0xca42b883b1f9481989cfb02fe693649f/). Here are some step-by-step instructions to replicate the worksheet.

* Initialize the worksheet: `cl work 'polymom-nips2015'`
* Pull the source from github: `cl upload --git https://github.com/sidaw/polymom`
* First try to convert the ipython notebook using the default `codalab/python` docker image. 
```
cl run -t --request-docker-image codalab/python ---\
 'cd %polymom%; 
  jupyter nbconvert --to html --allow-errors --execute --ExecutePreprocessor.timeout=1000000 
       MixtureLinearRegressions.ipynb;
  cp MixtureLinearRegressions.html ../main.html;
 cd ..';
```
** Note that we will need to copy the output to the parent directory to actually view the output.
** It is recommended that you use the '--allow-errors' command to be able to debug any errors encountered while executing the notebook.
* To view the output in the table, add the following lines to your worksheet source:
```
% schema nb
% add uuid uuid [0:8]
% add name
% add command
% add output uuid "key uuid | add path /main.html"
% add docker_image
% time time duration
% add state
% display table nb
[run mlr-notebook ...]
```
* At this point, we have encountered an error in our notebook: apparently the library 'ipdb' is not available on this image. If this is the case, we will need to create a docker image with the required dependencies.
```
docker run -ti --name polymom codalab/python bash 
$ pip install ipdb
$ exit
docker commit polymom arunchaganty/polymom # You will have to use your own username here.
docker push arunchaganty/polymom
```
* Rerun the `jupyter nbconvert` command with this new image.
```
cl run -t --request-docker-image arunchaganty/polymom ---\
 'cd %polymom%; 
  jupyter nbconvert --to html --allow-errors --execute --ExecutePreprocessor.timeout=1000000 
       MixtureLinearRegressions.ipynb;
  cp MixtureLinearRegressions.html ../main.html;
 cd ..';
```
* Profit!