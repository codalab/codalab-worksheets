Welcome to your **CodaLab Dashboard**, which shows worksheets and bundles
(programs and datasets) owned by you.  Read the
[documentation](https://github.com/codalab/codalab-worksheets/wiki) to learn
more.

## **My worksheets**
% wsearch .mine

## **My pending bundles**
These are bundles that are running or waiting to be run.
% display table run
% search .mine state=created,staged,making,waiting_for_worker_startup,starting,running id=.sort- .limit=10000

## **My recent bundles**
% search .mine id=.sort- .limit=10

## **My floating bundles**
These are bundles that are not on any worksheet (you might have lost track of these).
% search .mine .floating

## **Basic statistics**
Number of bundles owned by me:
% search .mine .count
My disk usage:
% search .mine size=.sum .format=size

This dashboard itself is a worksheet.  You can click 'Edit Source' and copy the
markdown to your own worksheet to customize it!
