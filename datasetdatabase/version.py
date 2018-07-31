# Format expected by setup.py and doc/source/conf.py: string of form "X.Y.Z"
_version_major = 0
_version_minor = 1
_version_micro = 5
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
AICS Modeling create and manage local and postgresql databases. It allows
arbitrary key value pairings to be tracked and managed through any processing
step, algorithm, or function.
Contact
=======
Jackson Maxfield Brown
jacksonb@alleninstitute.org
License
=======
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see http://www.gnu.org/licenses/.
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
            "psycopg2_binary",
            "python_Levenshtein"]
SCRIPTS = []
