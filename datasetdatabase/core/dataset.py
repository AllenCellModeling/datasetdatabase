#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pandas as pd
import collections
import types

# self
from .datasetdatabase import DatasetDatabase
from ..version import __version__
from ..utils import format
from ..utils import checks
from ..utils import tools

# globals
UNIX_REPLACE = {"\\": "/"}
MISSING_INIT = "Must provide either a pandas DataFrame or a DatasetInfo object."
MISSING_DSDB = "Missing a connection to a DatasetDatabase."

class Dataset(object):

    def __init__(self,
                 dataset: Union[pd.DataFrame, None] = None,
                 name: Union[str, None] = None,
                 description: Union[str, None] = None,
                 filepath_columns: Union[str, list] = [],
                 ds_info: Union[dict, None],
                 dsdb: Union[DatasetDatabase, None]):

        # enforce types
        checks.check_types(dataset, [pd.DataFrame, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(filepath_columns, [str, list])
        checks.check_types(ds_info, dict)
        checks.check_types(dsdb, [DatasetDatabase, type(None)])

        # must provide dataset or ds_info
        assert dataset is not None or ds_info is not None,


        # info and connection
        self._info = ds_info
        self.dsdb = dsdb

        if ds_info is not None:
            assert self.dsdb is not None, MISSING_DSDB

        # as dataframe
        elif dataset is not None:
            # TODO:
            # validate that
            self._dataframe = dataset
        else:
            self._dataframe = None

        # as both
        elif

    @property
    def info(self):
        return self._filepath_columns

    @property
    def filepath_columns(self):
        return self._filepath_columns


    @property
    def dataframe(self):
        if self._dataframe is None:
            self._dataframe = self.dsdb.get_dataset(self.info["DatasetId"])

        return self._dataframe

    def set_filepath_columns(self,
                             cols: Union[str, list] = [],
                             validate: bool = True,
                             upload: bool = False):

        # enforce types
        checks.check_types(cols, [str, list, type(None)])
        checks.check_types(validate, bool)
        checks.check_types(upload, bool)

        # convert types
        if isinstance(cols, str):
            cols = [cols]

        # store current
        prior_fp_cols = self._filepath_columns

        if self._filepath_columns is None:
            self._filepath_columns = cols


        if prior_fp_cols !=

    def validate(self,):
        format_paths = (filepath_columns is not None or
                        replace_paths is not None)
        store_paths = (filepath_columns is not None and store_files)
        steps = sum[import_as_type_map,
                    format_paths,
                    True,
                    True]

        total_validation_steps = (len(dataset) * len(dataset.columns) * steps)
        validation_bar = tools.ProgressBar(total_validation_steps)

        # format types
        if import_as_type_map:
            dataset = format.format_dataset(dataset,
                                            type_map,
                                            p_bar=validation_bar)

        # format paths
        if filepath_columns is not None or replace_paths is not None:
            dataset = format.format_paths(dataset,
                                          filepath_columns,
                                          replace_paths,
                                          p_bar=validation_bar)

        # check types
        checks.validate_dataset_types(dataset,
                                      type_map,
                                      filepath_columns,
                                      p_bar=validation_bar)

        # check values
        checks.validate_dataset_values(dataset,
                                       value_validation_map,
                                       p_bar=validation_bar)


    def process(self,
                algorithm: Union[types.MethodType, types.FunctionType],
                set_algorithm_name: Union[str, None] = None,
                set_algorithm_description: Union[str, None] = None,
                set_algorithm_version: Union[str, None] = None,
                name: Union[str, None] = None,
                description: Union[str, None] = None,
                alg_parameters: dict = {},
                dataset_parameters: dict = STANDARD_UPLOAD_PARAMS) -> Dataset:

        # enforce types
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(set_algorithm_name, [str, type(None)])
        checks.check_types(set_algorithm_description, [str, type(None)])
        checks.check_types(set_algorithm_version, [str, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(alg_parameters, dict)
        checks.check_types(dataset_parameters, dict)

        # create alg info before run
        alg_info = self.get_or_create_algorithm(algorithm,
                                                set_algorithm_name,
                                                set_algorithm_description,
                                                set_algorithm_version)

        # update description
        if description is None:
            description = "{a} run for {u}".format(a=alg_info["Name"],
                                                   u=self.dsdb.user)

        # create user info before run
        user_info = self.get_or_create_user(self.dsdb.user)

        # begin time
        begin = datetime.now()

        # use the dataset passed
        output_dataset = algorithm(self.dataframe, alg_parameters)

        # check output
        checks.check_types(output_dataset, pd.DataFrame)

        # create iota from new dataset
        if dataset_parameters != STANDARD_UPLOAD_PARAMS:
            dataset_parameters = {**STANDARD_UPLOAD_PARAMS,
                                  **dataset_parameters}

        dataset_parameters["dataset"] = output_dataset

        # create source
        source_info = self._insert_to_table("Source",
                        {"Created": datetime.now()})
        dataset_parameters["source_info"] = source_info
        if "source_type" not in dataset_parameters:
            dataset_parameters["source_type"] = "RunSource"
        if "source_type_id" not in dataset_parameters:
            dataset_parameters["source_type_id"] = None

        output = self._create_dataset(dataset_parameters)

        # end time
        end = datetime.now()

        processed_alg_parameters = self._reduce_complex_types(alg_parameters)

        # insert run info
        run_info = self._insert_to_table("Run",
                                        {"AlgorithmId": alg_info["AlgorithmId"],
                                         "UserId": user_info["UserId"],
                                         "Name": name,
                                         "Description": description,
                                         "AlgorithmParameters":
                                            str(processed_alg_parameters),
                                         "Begin": begin,
                                         "End": end})

        # insert run input
        self._insert_to_table("RunInput",
                              {"RunId": run_info["RunId"],
                              "DatasetId": self.info["DatasetId"],
                              "Created": datetime.now()})

        # insert run output
        self._insert_to_table("RunOutput",
                              {"RunId": run_info["RunId"],
                               "DatasetId": output.info["DatasetId"],
                               "Created": datetime.now()})

        # insert run source
        if dataset_parameters["source_type"] in ["RunSource"]:
            self._insert_to_table("RunSource",
                                  {"SourceId": source_info["SourceId"],
                                   "RunId": run_info["RunId"],
                                   "Created": datetime.now()})

        return output
