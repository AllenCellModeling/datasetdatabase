#!/usr/bin/env python

# self
from .schemaversion import SchemaVersion
from ..schema import tables

from ..version import VERSION

# globals
# CREATION ORDER OF TABLES MATTERS
TABLES = {"User": tables.create_User,
          "Iota": tables.create_Iota,
          "Group": tables.create_Group,
          "Source": tables.create_Source,
          "FileSource": tables.create_FileSource,
          "QuiltSource": tables.create_QuiltSource,
          "Dataset": tables.create_Dataset,
          "IotaGroup": tables.create_IotaGroup,
          "GroupDataset": tables.create_GroupDataset,
          "Algorithm": tables.create_Algorithm,
          "Run": tables.create_Run,
          "RunInput": tables.create_RunInput,
          "RunOutput": tables.create_RunOutput,
          "RunSource": tables.create_RunSource}

MINIMAL = SchemaVersion("MINIMAL", TABLES, VERSION)
