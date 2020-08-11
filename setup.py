#!/usr/bin/env python

# Remove .egg-info directory if it exists, to avoid dependency problems with
# partially-installed packages (20160119/dphiffer)

import os, sys
from shutil import rmtree

cwd = os.path.dirname(os.path.realpath(sys.argv[0]))
egg_info = cwd + "/mapzen.whosonfirst.elasticsearch.egg-info"
if os.path.exists(egg_info):
    rmtree(egg_info)

from setuptools import setup, find_packages

packages = find_packages()
desc = open("README.md").read(),
version = open("VERSION").read()

setup(
    name='mapzen.whosonfirst.elasticsearch',
    namespace_packages=['mapzen', 'mapzen.whosonfirst' ],
    python_requires='>3',    
    version=version,
    description='',
    author='Mapzen',
    url='https://github.com/mapzen/py-mapzen-whosonfirst-elasticsearch',
    packages=packages,
    scripts=[
        ],
    download_url='https://github.com/mapzen/py-mapzen-whosonfirst-elasticsearch/releases/tag/' + version,
    license='BSD')
