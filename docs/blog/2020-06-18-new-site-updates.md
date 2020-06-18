# New site and updates from the past year

June 8, 2020

The CodaLab Team

We'll now be posting major announcements and changes to CodaLab on this blog. We'll also be sending out an occasional email digest to all CodaLab Worksheets users with the updates that are posted to this blog. If you're interested in being added to the email list, please email <a href="mailto:codalab.worksheets@gmail.com">codalab.worksheets@gmail.com</a>.

Here are some of the major new features that we have released within the past few months:

## Frontend improvements

- We've introduced a new frontend that allows you to edit cells directly from the UI without having to go and edit source.

![Editing](https://user-images.githubusercontent.com/1689183/84437569-36a6c580-ac03-11ea-8f9e-91adff5747df.png)

- The worksheet header is now sticky, so the main buttons to add text / upload / run are easily accessible from the top.

![Sticky header](https://user-images.githubusercontent.com/1689183/84438067-f0059b00-ac03-11ea-8a85-b91bd3c4b0d5.png)

- Worksheet loading is now faster, after the addition of async loading of file contents ([#2246](https://github.com/codalab/codalab-worksheets/pull/2246)) and search result blocks ([#2086](https://github.com/codalab/codalab-worksheets/pull/2086)).

![Async loading](https://user-images.githubusercontent.com/1689183/84437655-5a6a0b80-ac03-11ea-8dec-0b6aa7aea6e7.png)

- Added a search bar for worksheets, allowing you to more easily access the worksheets you need ([#2181](https://github.com/codalab/codalab-worksheets/pull/2181)).

![Search bar](https://user-images.githubusercontent.com/1689183/84437455-02cba000-ac03-11ea-93cf-67119ea0e4e6.png)

- The addition of a cut / copy / paste feature has made it easier to manage worksheets exclusively by interacting through the frontend GUI ([#2009](https://github.com/codalab/codalab-worksheets/pull/2009), [#2143](https://github.com/codalab/codalab-worksheets/pull/2143)).

![Cut copy paste](https://user-images.githubusercontent.com/1689183/84437547-2c84c700-ac03-11ea-9a06-2d27335fa57a.png)

- Users can now upload directories and multiple files from the browser ([#2043](https://github.com/codalab/codalab-worksheets/pull/2043)).

![Upload](https://user-images.githubusercontent.com/1689183/84437824-9d2be380-ac03-11ea-8e51-ceee3f624e1b.png)


## Tooling

- We've set up frontend automation testing using Selenium, allowing us to catch regressions earlier ([#2028](https://github.com/codalab/codalab-worksheets/pull/2028)).

- We regularly run stress tests on the CodaLab backend before releases to make sure that it can handle large files and large loads ([#1810](https://github.com/codalab/codalab-worksheets/pull/1810)).


## Future

We're focusing on ways to improve the usability of CodaLab Worksheets. Here are some of those initiatives we are working on:

- Removing limitations on mounting dependencies ([#1115](https://github.com/codalab/codalab-worksheets/issues/1115))
- Externalizing storage. This involves:
  - Adding the `--link` CLI argument so that users can use files with CodaLab without uploading it to the system ([#2322](https://github.com/codalab/codalab-worksheets/issues/2322))
  - Allowing using storage backends such as S3, Azure Blob Storage, GCS, or HTTP URLs to import bundles ([#2327](https://github.com/codalab/codalab-worksheets/issues/2327))
- Creating an `--interactive` mode so that users can use CodaLab more easily
- Switching our CI tool from Travis to GitHub Actions to speed up the contribution / release process

We couldn't have done this without all the feedback from users and help from contributors! [Open an issue](https://github.com/codalab/codalab-worksheets/issues/new/choose) if there is something else you would like to see in CodaLab Worksheets.