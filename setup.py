#!/usr/bin/env python

"""
@file setup.py
@author Dave Foster <dfoster@asascience.com>
@brief setup file for ANF
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

setupdict = {
    'name' : 'anf',
    'version' : '0.1', #VERSION,
    'description' : 'OOI ANF Seismic Application',
    'url': 'https://confluence.oceanobservatories.org/display/CIDev/ANF-SQLstream+Integration',
    'download_url' : 'http://ooici.net/packages',
    'license' : 'Apache 2.0',
    'author' : 'Dave Foster',
    'author_email' : 'dfoster@asascience.com',
    'keywords': ['ooici','anf'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering'],
}

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()

setupdict['dependency_links'] = ['http://ooici.net/packages']
#setupdict['test_suite'] = 'anf'

setupdict['install_requires'] = ['ioncore==0.4.3']

setupdict['include_package_data'] = True
setupdict['package_data'] =  {
    'anf': ['data/*.sqlt', 'data/install_sqlstream.sh']
}
setup(**setupdict)

