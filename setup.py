from setuptools import setup, find_packages
import setuptools

# should match codalab/common.py#CODALAB_VERSION
CODALAB_VERSION = "0.4.0"

if int(setuptools.__version__.split('.')[0]) < 25:
    print(
        "WARNING: Please upgrade setuptools to a newer version, otherwise installation may break. "
        "Recommended command: `pip install -U setuptools`"
    )


def get_requirements(*requirements_file_paths):
    requirements = ['codalabworker>={}'.format(CODALAB_VERSION)]
    for requirements_file_path in requirements_file_paths:
        with open(requirements_file_path) as requirements_file:
            for line in requirements_file:
                if line[0:2] != '-r' and line.find('git') == -1:
                    requirements.append(line.strip())
    return requirements


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
    include_package_data=True,
    install_requires=get_requirements('worker/requirements.txt'),
    entry_points={
        'console_scripts': ['cl=codalab.bin.cl:main', 'codalab-service=codalab_service:main']
    },
    zip_safe=False,
),
