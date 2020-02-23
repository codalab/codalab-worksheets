import argparse
import os
import time

from abc import ABC, abstractmethod
from diffimg import diff
from selenium import webdriver
from selenium.webdriver import ChromeOptions, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


class UITester(ABC):
    _SCREENSHOT_DIFF_THRESHOLD_PERCENT = 9
    _BASE_PATH = os.path.dirname(os.path.abspath(__file__))

    def __init__(self, test_name, base_url='http://localhost'):
        self._test_name = test_name
        self._base_url = base_url

    @abstractmethod
    def test(self):
        pass

    def run(self):
        def add_headless(browser_options):
            if args.headless:
                browser_options.add_argument('--headless')

        # Test Firefox
        options = FirefoxOptions()
        add_headless(options)
        self._driver = webdriver.Firefox(log_path='', firefox_options=options)
        self.test()
        self._driver.close()

        # Test Chrome
        options = ChromeOptions()
        add_headless(options)
        self._driver = webdriver.Chrome(chrome_options=options)
        self.test()
        self._driver.close()

    def login(self, username='codalab', password='codalab'):
        self._driver.get(self.get_url('/home'))
        self.click_link('LOGIN')
        self.fill_field('id_login', username)
        password_field = self.fill_field('id_password', password)
        password_field.send_keys(Keys.ENTER)

    def save_screenshot(self, path, filename):
        self._driver.save_screenshot(os.path.join(path, filename))

    def click_link(self, selector):
        link = self._driver.find_element_by_link_text(selector)
        link.click()

    def fill_field(self, selector, text):
        textbox = self._driver.find_element_by_id(selector)
        textbox.send_keys(text)
        return textbox

    def wait_until_worksheet_loads(self):
        self.wait_until_page_loads('ws-item')

    def wait_until_page_loads(self, selector, by=By.CLASS_NAME):
        timeout_message = 'Timed out while waiting for {}: {}.'.format(by, selector)
        return WebDriverWait(self._driver, 15).until(
            EC.presence_of_element_located((by, selector)), message=timeout_message
        )

    def switch_to_new_tab(self):
        # Just give enough time for the new tab to get opened
        time.sleep(1)
        self._driver.switch_to_window(
            self._driver.window_handles[len(self._driver.window_handles) - 1]
        )

    def output_images(self, selector, num_of_screenshots=10):
        output_dir = self._get_output_dir('out')
        element = "document.getElementById('{}')".format(selector)
        scroll_height = float(self._driver.execute_script('return {}.scrollHeight'.format(element)))
        for i in range(num_of_screenshots):
            y = (i / num_of_screenshots) * scroll_height
            self._driver.execute_script('{}.scrollTo(0, {})'.format(element, y))
            self.save_screenshot(output_dir, '{}{}.png'.format(self._test_name, i + 1))

    def compare_to_baselines(self, num_of_screenshots=10):
        out_dir = self._get_output_dir('out')
        baselines_dir = self._get_output_dir('baselines')
        diff_dir = self._get_output_dir('diff')
        has_failed = False
        for i in range(num_of_screenshots):
            screenshot_filename = '{}{}.png'.format(self._test_name, i + 1)
            out_img = os.path.join(out_dir, screenshot_filename)
            baseline_img = os.path.join(baselines_dir, screenshot_filename)
            diff_img = os.path.join(diff_dir, screenshot_filename)
            diff_percent = (
                diff(baseline_img, out_img, delete_diff_file=True, ignore_alpha=True) * 100
            )
            if diff_percent > UITester._SCREENSHOT_DIFF_THRESHOLD_PERCENT:
                # If an image comparison has failed, generate diff and print an error message in red
                has_failed = True
                diff(
                    out_img,
                    baseline_img,
                    delete_diff_file=False,
                    diff_img_file=diff_img,
                    ignore_alpha=True,
                )
                print(
                    '\033[91mScreenshot comparison failed in {} for {} by {}%\033[0m'.format(
                        self._get_browser(), screenshot_filename, diff_percent
                    )
                )

        assert not has_failed

    def get_url(self, path):
        return '{}/{}'.format(self._base_url, path)

    def _get_output_dir(self, folder_name):
        def create_path(path):
            if not os.path.isdir(path):
                os.mkdir(path)

        output_dir = os.path.join(UITester._BASE_PATH, folder_name)
        create_path(output_dir)
        output_dir = os.path.join(output_dir, self._test_name)
        create_path(output_dir)
        output_dir = os.path.join(output_dir, self._get_browser())
        create_path(output_dir)
        return output_dir

    def _get_browser(self):
        return self._driver.capabilities['browserName']


class WorksheetTest(UITester):
    def __init__(self):
        super().__init__('worksheet')

    def test(self):
        self.login()
        self.wait_until_worksheet_loads()
        self.click_link('Small Worksheet [cl_small_worksheet]')
        self.switch_to_new_tab()
        self.wait_until_worksheet_loads()
        self.output_images('worksheet_container')
        self.compare_to_baselines()


def main():
    # Add ui tests here and run them
    all_tests = [WorksheetTest()]

    start_time = time.time()
    for test in all_tests:
        test.run()
    duration_seconds = time.time() - start_time
    print('Success.')
    print('\n--- Completion Time: {} minutes---'.format(duration_seconds / 60))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run screenshot tests for the CodaLab UI')
    parser.add_argument(
        '--headless', action='store_true', help='Whether to test using headless browsers'
    )
    args = parser.parse_args()
    main()
