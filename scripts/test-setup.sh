#!/bin/bash

set -e

rm -rf geckodriver chromedriver

# For testing CodaLab in Chrome
wget https://chromedriver.storage.googleapis.com/92.0.4515.107/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver
rm chromedriver_linux64.zip

# For testing CodaLab in Firefox
wget https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz
mkdir geckodriver
tar -xzf geckodriver-v0.26.0-linux64.tar.gz -C geckodriver
chmod +x geckodriver/geckodriver
rm geckodriver-v0.26.0-linux64.tar.gz
sudo mv geckodriver/geckodriver /usr/bin/geckodriver
rm -r geckodriver