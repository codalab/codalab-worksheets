## CodaLab UI Tests

We test the CodaLab UI by using [Selenium](https://pypi.org/project/selenium/#history) and 
[diffimg](https://pypi.org/project/diffimg/#history) for our screenshots comparison tool. Currently, our front end 
automation only runs in Chrome and Firefox. We hope to support more browsers in the future. 

### Setting up the tests on your local machine

#### One-time setup

1. If you haven't already, install the Python dependencies specified in `requirements.txt`, located in the root of the 
repository, by running `pip install -r requirements.txt`.
2. Next, use one of the two below options to install [ChromeDriver](https://chromedriver.chromium.org/getting-started) (for Chrome) and 
[GeckoDriver](https://github.com/mozilla/geckodriver) (for Firefox). These drivers are the links between the Selenium 
tests and their respective browsers.
    1. If on Linux, run `./scripts/test-setup.sh`.
    2. An alternative is to simply use [Homebrew](https://brew.sh/) and run `brew install geckodriver && brew install 
    --cask chromedriver`. 

#### Running the tests locally

1. From the root of the repository, start the CodaLab service by running `./codalab_service.py start -bd`.
2. Populate your local machine with a comprehensive sample worksheet by running 
`docker exec -it codalab_rest-server_1 /bin/bash -c "python3 scripts/create_sample_worksheet.py"`. 
3. Start the UI tests by running `python tests/ui/ui_tester.py`. You can also pass in an additional argument 
`--headless`, which will run the tests in headless mode.
4. If you want to run the UI tests directly within the CodaLab Docker container instead, you should run `docker exec codalab_rest-server_1 /bin/bash -c "python3 scripts/create_sample_worksheet.py --test-print" && python test_runner.py frontend`.

### Maintaining the baseline images

If the screenshot tests are failing, check the `diff` folder for the diff images that are generated when image 
comparisons fail. If the output images is as expected, accept them as the new baselines by replacing the stale baseline 
images, located in `baselines`, with the newly generated images in `out`. If the failures are not expected, look into 
your code changes, as they may have cause the tests to fail.

### Adding new tests
   
To add a new test, define a class that inherits the base class `UITester` in `ui_tester.py` and implement the required 
`test` method. The test method contains the instructions on how Selenium interacts with CodaLab and the acceptance
criteria for the specific test. The base abstract class `UITester` holds common logic that can be used across different 
UI tests (e.g. a method that logs into CodaLab).

Below is an example test that takes screenshots of the sample worksheet and compares them to the existing baseline 
images.

```python
class WorksheetUITester(UITester):
    def __init__(self):
        # The name of this test is "worksheet".
        super().__init__('worksheet')

    def test(self):
        self.login()
        self.wait_until_worksheet_loads()
        self.click_link('Small Worksheet [cl_small_worksheet]')
        self.switch_to_new_tab()
        self.wait_until_worksheet_loads()
        self.output_images('worksheet_container')
        self.compare_to_baselines()
```  

If your test relies on screenshot comparisons, make sure to run your test locally and accept the generated screenshots
as the baseline by adding it to the `baselines` folder.