from setuptools import setup

setup(name='codalab-cli',
    version='0.0.1',
    description='CLI for CodaLab, a platform for reproducible computation',
    long_description='Visit https://worksheets.codalab.org/ or setup your own instance',
    url='https://github.com/codalab/codalab-cli',
    author='CodaLab',
    author_email='codalab.worksheets@gmail.com', # Percy's email?
    license='Apache License 2.0',
    keywords='codalab reproducible computation worksheets competitions',
    packages=['codalab'],
    install_requires=[
        'argcomplete==1.1.0',
        'PyYAML==3.11',
        'psutil==3.3.0',
        'six==1.10.0',
        'SQLAlchemy==1.0.8',
        'watchdog==0.8.3',
    ],
    scripts=[
        'codalab/bin/cl'
    ],
    zip_safe=False),
