Welcome to your **CodaLab Dashboard**, which shows worksheets and bundles (programs and datasets) owned by you.  Read the [tutorial](https://github.com/codalab/codalab-worksheets/wiki/User_CodaLab-Worksheets-Tutorial) to learn more.

## **My worksheets**
% wsearch .mine

To create a new worksheet, click the web terminal above and type:

    cl new <worksheet-name>

and then click 'Edit Source' to edit the markdown (see [syntax](https://github.com/codalab/codalab-worksheets/wiki/User_Worksheet-Markdown)). We have already created your home worksheet for you (see above).

## **My recent bundles**
% search created=.sort- .limit=5 .mine

To upload a program or dataset, click 'Upload bundle' on the right.
To run a command, click the web terminal above and type (for example):
    
    cl run 'echo hello'

To see a more complex example with dependencies, check out the [tutorial](https://github.com/codalab/codalab-worksheets/wiki/User_CodaLab-Worksheets-Tutorial).

## **My running bundles**
These are bundles that are currently running or queued to be run.
% schema r
% add uuid uuid [0:8]
% add name
% add owner owner_name
% add created created date
% add time time duration
% add state
% display table r
% search state=running created=.sort- .limit=10000 .mine
% search state=queued created=.sort- .limit=10000 .mine

## **My floating bundles**
These are bundles that are not on any worksheet (you might have lost track of these).
% search .mine .floating

## **Basic statistics**
Number of bundles owned by me:
% search .mine .count
My disk usage:
% search .mine size=.sum .format=size

This dashboard itself is a worksheet.  You can click 'Edit Source' and copy the markdown to your own worksheet to customize it!
