# Format expected by setup.py and doc/source/conf.py: string of form "X.Y.Z"
_version_major = 0
_version_minor = 8
_version_micro = 1
_version_extra = 'dev'

# Construct full version string from these.
_ver = [_version_major, _version_minor]
if _version_micro:
    _ver.append(_version_micro)
if _version_extra:
    _ver.append(_version_extra)

__version__ = '.'.join(map(str, _ver))

CLASSIFIERS = ["Development Status :: 1 - Alpha",
               "Environment :: Console",
               "Intended Audience :: Science/Research",
               "License :: OSI Approved :: GNU License",
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
`DatasetDatabase` is licensed under the terms of the MIT license. See the file
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
LICENSE = "GNU"
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
            "pathlib",
            "networkx",
            "matplotlib",
            "sqlalchemy",
            "psycopg2_binary",
            "python_Levenshtein"]
SCRIPTS = []
