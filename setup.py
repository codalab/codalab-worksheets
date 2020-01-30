from setuptools import setup, find_packages
from setuptools.command.install import install

import os
import setuptools


# Should match codalab/common.py#CODALAB_VERSION
CODALAB_VERSION = "0.5.7"


class Install(install):
    _WARNING_TEMPLATE = (
        '\n\n\033[1m\033[93mWarning! CodaLab was installed at {}, which is not\n'
        'one of the following paths in PATH:\n\n{}\n\nConsider adding {} to be in the path in order\n'
        'to be able to run commands using the command-line interface.\033[0m\n\n'
    )

    def run(self):
        install.run(self)
        self._check_path()

    def _check_path(self):
        cl_path = self.install_scripts
        executable_paths = os.environ['PATH'].split(os.pathsep)
        if cl_path not in executable_paths:
            # Prints a yellow, bold warning message in regards to the installation path not in $PATH
            print(Install._WARNING_TEMPLATE.format(cl_path, '\n'.join(executable_paths), cl_path))


def get_requirements(*requirements_file_paths):
    requirements = []
    for requirements_file_path in requirements_file_paths:
        with open(requirements_file_path) as requirements_file:
            for line in requirements_file:
                if line[0:2] != '-r' and line.find('git') == -1:
                    requirements.append(line.strip())
    return requirements


if int(setuptools.__version__.split('.')[0]) < 25:
    print(
        "WARNING: Please upgrade setuptools to a newer version, otherwise installation may break. "
        "Recommended command: `pip3 install -U setuptools`"
    )

setup(
    name='codalab',
    version=CODALAB_VERSION,
    description='CLI for CodaLab, a platform for reproducible computation',
    long_description=(
        'Visit https://worksheets.codalab.org/ or setup your own server by following the '
        'instructions in the documentation (https://codalab-worksheets.readthedocs.io/en/latest/Server-Setup).'
    ),
    url='https://github.com/codalab/codalab-worksheets',
    author='CodaLab',
    author_email='codalab.worksheets@gmail.com',
    license='Apache License 2.0',
    keywords='codalab reproducible computation worksheets competitions',
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires='~=3.6',
    cmdclass={'install': Install},
    include_package_data=True,
    install_requires=get_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'cl=codalab.bin.cl:main',
            'cl-server=codalab.bin.server:main',
            'cl-bundle-manager=codalab.bin.bundle_manager:main',
            'codalab-service=codalab_service:main',
            'cl-worker=codalab.worker.main:main',
            'cl-worker-manager=codalab.worker_manager.main:main',
            'cl-competitiond=scripts.competitiond:main',
        ]
    },
    zip_safe=False,
),
