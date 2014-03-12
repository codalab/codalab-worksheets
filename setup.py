from distutils.core import setup
from setuptools import setup

setup(
    name='codalab-cli',
    version='0.1.9',
    author='Codalab, Shaunak Kishore, Justin Carden',
    author_email='jecarden@stanford.edu',
    packages=['codalab', 'codalab.bundles', 'codalab.client', 'codalab.config',
              'codalab.lib','codalab.model','codalab.objects','codalab.server'],
    scripts=['codalab/bin/codalab_client.py','codalab/bin/codalab_server.py'],
    url='http://pypi.python.org/pypi/codalab/',
    license='LICENSE.txt',
    description='Codalab CLI is a command-line tool for interacting with Codalab. See http://codalab.org/',
    long_description=open('README.txt').read(),
    install_requires=[
                       'SQLAlchemy >=0.8.3'
    ],
    entry_points={'console_scripts':
                  ['codalab = bin.codalab_client.py:main',
                   'codalab_worker = bin.codalab_server.py:main']}
)
