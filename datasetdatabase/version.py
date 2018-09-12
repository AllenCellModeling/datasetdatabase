# Format expected by setup.py and doc/source/conf.py: string of form "X.Y.Z"
_version_major = 1
_version_minor = 0
_version_micro = 3
_version_extra = 'stable'

# Construct full version string from these.
_ver = [_version_major, _version_minor]
if _version_micro:
    _ver.append(_version_micro)
if _version_extra:
    _ver.append(_version_extra)

__version__ = '.'.join(map(str, _ver))

CLASSIFIERS = ["Development Status :: 5 - Production/Stable",
               "Environment :: Console",
               "Intended Audience :: Science/Research",
               "License :: OSI Approved :: BSD License",
               "Operating System :: OS Independent",
               "Programming Language :: Python",
               "Topic :: Scientific/Engineering"]

# Description should be a one-liner:
description = "dataset database: scripts and functions to create and manage a \
database for tracking arbitrary datasets"
# Long description will go up on the pypi page
long_description = """
dataset database
========
dataset database is the tools and functions library built to help
AICS Modeling create and manage local sqlite and postgresql databases. It allows
arbitrary key value pairings to be tracked and managed through any processing
step, algorithm, or function.
Contact
=======
Jackson Maxfield Brown
jacksonb@alleninstitute.org
License
=======
`DatasetDatabase` is licensed under the terms of the BSD-2 license. See the file
"LICENSE" for information on the history of this software, terms & conditions
for usage, and a DISCLAIMER OF ALL WARRANTIES.

All trademarks referenced herein are property of their respective holders.
Copyright (c) 2018--, Jackson Maxfield Brown, The Allen Institute for Cell
Science.
"""

NAME = "datasetdatabase"
MAINTAINER = "Jackson Maxfield Brown"
MAINTAINER_EMAIL = "jacksonb@alleninstitute.org"
DESCRIPTION = description
LONG_DESCRIPTION = long_description
LICENSE = "BSD-2"
AUTHOR = "Jackson Maxfield Brown"
AUTHOR_EMAIL = "jacksonb@alleninstitute.org"
PLATFORMS = "OS Independent"
MAJOR = _version_major
MINOR = _version_minor
MICRO = _version_micro
VERSION = __version__
REQUIRES = ["numpy",
            "quilt",
            "pandas",
            "orator",
            "networkx",
            "pathlib",
            "matplotlib",
            "sqlalchemy",
            "psycopg2_binary",
            "python_Levenshtein"]
INSTALLS = ["numpy>=1.15.1",
            "quilt>=2.9.10",
            "pandas>=0.23.4",
            "orator>=0.9.7",
            "networkx>=2.1",
            "pathlib>=1.0.1",
            "matplotlib>=2.2.3",
            "sqlalchemy>=1.2.10",
            "psycopg2_binary>=2.7.5",
            "python_Levenshtein>=0.12.0"]
SCRIPTS = []
