#!/bin/env python
import os

from pip.req import parse_requirements
from setuptools import setup


setup(
    name='cache',
    use_scm_version=False,
    version='0.1',
    url='https://github.com/axado/hadouken/fury-experiments/client_cache/cache',
    description="Client in python for Cache",
    long_description='''Client Python for Fury Cache''',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    include_package_data=True,
    packages=['cache'],
    keywords=['python', 'cache'],
    install_requires=[
        'requests==2.18.4',
        'urllib3==1.22',
        'ipdb',
    ],
    dependency_links=[],
    setup_requires=['setuptools_scm'],
    tests_require=[],
)
