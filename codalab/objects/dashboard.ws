Welcome to your **CodaLab Dashboard**, which shows worksheets and bundles 
(programs and datasets) owned by you.  Read the 
[documentation](https://codalab-worksheets.readthedocs.io/en/latest) to 
learn more. See [this page](https://codalab-worksheets.readthedocs.io/en/latest/Worksheet-Markdown)
to learn more about CodaLab's markdown syntax. 

## **My worksheets**
% wsearch .mine

## **My running bundles**
These are bundles that are currently running or queued to be run.
% schema r
% add uuid uuid '[0:8]'
% add name
% add owner owner_name
% add created created date
% add time time duration
% add state
% display table r
% search state=running created=.sort- .limit=10000 .mine
% search state=staged created=.sort- .limit=10000 .mine

## **My recent bundles**
% search id=.sort- .limit=5 .mine

## **My floating bundles**
These are bundles that are not on any worksheet (you might have lost track of these).
% search .mine .floating

## **Basic statistics**
Number of bundles owned by me:
% search .mine .count
My disk usage:
% search .mine size=.sum .format=size

This dashboard itself is a worksheet.  You can click 'Edit Source' and copy the markdown to your own worksheet to customize it!
