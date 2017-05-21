from setuptools import setup, find_packages
from setuptools.command.install import install
import os
# from codalab.common import CODALAB_VERSION

def get_requirements(requirements_file_path):
    with open(requirements_file_path) as requirements_file:
        return [line for line in requirements_file]

setup(name='codalabworker',
    version='0.2.6',
    description='Worker for CodaLab, a platform for reproducible computation',
    long_description='To use your own hardware in CodaLab Worksheets, visit https://github.com/codalab/codalab-worksheets/wiki/Execution#running-your-own-workervisit. You can find the code at https://github.com/codalab/codalab-cli.',
    url='https://github.com/codalab/codalab-cli',
    author='CodaLab',
    author_email='codalab.worksheets@gmail.com',
    license='Apache License 2.0',
    keywords='codalab reproducible computation worksheets competitions worker',
    packages=find_packages(include=['codalabworker*']),
    package_data={'': 'requirements.txt'},
    include_package_data=True,
    install_requires=get_requirements('./requirements.txt'),
    entry_points={
        'console_scripts': [
            'cl-worker=codalabworker.main:main',
        ],
    },
    zip_safe=False),
