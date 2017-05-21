from setuptools import setup, find_packages
from setuptools.command.install import install
import os
from codalab.common import CODALAB_VERSION

def get_requirements(*requirements_file_paths):
    requirements = []
    for requirements_file_path in requirements_file_paths:
        with open(requirements_file_path) as requirements_file:
            for line in requirements_file:
                if line[0:2] != '-r' and line.find('git') == -1:
                    requirements.append(line.strip())
    return requirements

def get_dependency_links(*requirements_file_paths):
    dependency_links = []
    for requirements_file_path in requirements_file_paths:
        with open(requirements_file_path) as requirements_file:
            for line in requirements_file:
                if line.find('git') != -1:
                    dependency_links.append(line.strip())
    return dependency_links

setup(name='codalab',
    version=CODALAB_VERSION,
    description='CLI for CodaLab, a platform for reproducible computation',
    long_description='Visit https://worksheets.codalab.org/ or setup your own server by following the instructions in the Wiki (https://github.com/codalab/codalab-worksheets/wiki/Server-Setup).',
    url='https://github.com/codalab/codalab-cli',
    author='CodaLab',
    author_email='codalab.worksheets@gmail.com',
    license='Apache License 2.0',
    keywords='codalab reproducible computation worksheets competitions',
    packages=find_packages(include=['codalab*',]),
    include_package_data=True,
    install_requires=get_requirements('./requirements.txt', './requirements-server.txt'),
    dependency_links=get_dependency_links('./requirements.txt', './requirements-server.txt'),
    entry_points={
        'console_scripts': [
            'cl=codalab.bin.cl:main',
        ],
    },
    zip_safe=False),
