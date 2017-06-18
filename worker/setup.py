from setuptools import setup, find_packages
from setuptools.command.install import install
import os

def get_requirements(*requirements_file_paths):
    requirements = []
    for requirements_file_path in requirements_file_paths:
        with open(requirements_file_path) as requirements_file:
            for line in requirements_file:
                if line[0:2] != '-r' and line.find('git') == -1 and line.find('codalabworker') == -1:
                    requirements.append(line)
    return requirements

setup(name='codalabworker',
    version='0.2.13',
    description='Worker for CodaLab, a platform for reproducible computation',
    long_description='To use your own hardware in CodaLab Worksheets, visit https://github.com/codalab/codalab-worksheets/wiki/Execution#running-your-own-worker. You can find the code at https://github.com/codalab/codalab-cli.',
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
