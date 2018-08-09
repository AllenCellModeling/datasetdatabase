#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union

# self
from ..utils import checks

FILEPATH_COL_SEP = ","


class DatasetInfo(object):
    def __init__(self,
                 DatasetId: int,
                 Name: str,
                 Description: Union[str, None] = None,
                 FilepathColumns: Union[str, None] = None,
                 SourceId: int,
                 Created: datetime):

        # enforce types
        checks.check_types(DatasetId, int)
        checks.check_types(Name, str)
        checks.check_types(Description, [str, type(None)])
        checks.check_types(FilepathColumns, [str, type(None)])
        checks.check_types(SourceId, int)
        checks.check_types(Created, datetime)

        # convert types
        if Description is None:
            Description = ""
        if FilepathColumns is None:
            FilepathColumns = []
        if isinstance(FilepathColumns, str):
            FilepathColumns = FilepathColumns.split(FILEPATH_COL_SEP)

        self._id = DatasetId
        self._name = Name
        self._description = Description
        self._fp_cols = FilepathColumns
        self._source_id = SourceId
        self._created = Created


    @property
    def id(self):
        return self._id


    @property
    def name(self):
        return self._name


    @property
    def description(self):
        return self._description


    @property
    def filepath_columns(self):
        return self._fp_cols


    @property
    def source_id(self):
        return self._source_id


    @property
    def created(self):
        return self._created


    def __str__(self):
        return str({"id": self.id,
                    "name": self.name,
                    "description": self.description,
                    "filepath_columns": self.filepath_columns,
                    "source_id": self.source_id,
                    "created": self.created})


    def __repr__(self):
        return str(self)
