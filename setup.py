"""Setup module for aws-batch-helpers"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='aws-batch-helpers',
    version='0.2',
    description='Tools for analysis workflows on AWS Batch',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/FredHutch/aws-batch-helpers',
    author='Samuel Minot',
    author_email='sminot@fredhutch.org',
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='docker aws',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        "boto3>=1.7.2",
        "pandas>=0.20.3",
        "tabulate==0.8.1"
    ],
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/fredhutch/aws-batch-helpers/issues',
        'Source': 'https://github.com/fredhutch/aws-batch-helpers/',
    },
    entry_points={
        'console_scripts': [
            'batch_project = batch_project.main:main',
            'batch_dashboard = batch_project.main:dashboard',
            'batch_queue_status = batch_project.main:queue_status',
            'batch_clear_queue = batch_project.main:clear_queue',
        ],
    },
)
