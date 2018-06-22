# installed
from orator.exceptions.query import QueryException
from datetime import datetime
from getpass import getuser
import pandas as pd
import numpy as np
import pathlib
import orator
import time
import os

# self
from .utils import checks
from .utils import tools
from .utils import admin
from .query import *

def upload_dataset(database,
                   dataset,
                   name=None,
                   description=None,
                   type_map=None,
                   validation_map=None,
                   upload_files=False,
                   filepath_columns=None,
                   import_as_type_map=False,
                   use_unix_paths=True):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
    checks.check_types(name, [str, type(None)])
    checks.check_types(description, [str, type(None)])
    checks.check_types(type_map, [dict, type(None)])
    checks.check_types(validation_map, [dict, type(None)])
    checks.check_types(upload_files, bool)
    checks.check_types(filepath_columns, [str, list, pd.Series, type(None)])
    checks.check_types(import_as_type_map, bool)
    checks.check_types(use_unix_paths, bool)

    # convert types
    if isinstance(dataset, str):
        dataset = pathlib.Path(dataset)

    if isinstance(filepath_columns, str):
        filepath_columns = [filepath_columns]

    # get user
    user = getuser()
    if user in ["admin", "root", "jovyan"] and "DOCKER_USER" in os.environ:
        user = os.environ["DOCKER_USER"]

    # check dataset name
    if name is None:
        if isinstance(dataset, pathlib.Path):
            name = str(dataset) + "@@" + str(datetime.now())
        else:
            name = user + "@@" + str(datetime.now())

    # read dataset
    if isinstance(dataset, pathlib.Path):
        dataset = pd.read_csv(dataset)

        # TODO:
        # handle fms upload and get guid
        sourceid = "1"
        sourcetypeid = 1

    else:
        sourceid = name
        sourcetypeid = 3

    # validate dataset
    if import_as_type_map:
        dataset = handles.format_data(dataset, type_map)

    dataset = handles.format_paths(dataset, filepath_columns, use_unix_paths)
    checks.validate_dataset(dataset, type_map, filepath_columns)
    checks.enforce_values(dataset, validation_map)

    # actual dataset name check
    found_ds = get_items_in_table(database, "Dataset", {"Name": name})
    if len(found_ds) > 0:
        print("A dataset with that name already exists. Adding new version.")
        name += "@@" + str(datetime.now())
        sourceid = name

    # add user if not exists
    if len(get_items_in_table(database, "User", {"Name": user})) > 0:
        admin.add_user(database, user)

    # iter dataset and insert iota
    iota = {}
    current_i = 0
    start_time = time.time()
    total_upload = len(dataset)
    for groupid, row in dataset.iterrows():
        # TODO:
        # handle filepaths and fms upload
        # use get userid

        # update progress
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # parse row
        r = dict(row)
        for key, value in r.items():
            if isinstance(value, str):
                value = value.replace("\n", "")

            to_add = {"SourceId": sourceid,
                      "SourceTypeId": sourcetypeid,
                      "GroupId": groupid,
                      "Key": str(key),
                      "Value": str(value),
                      "ValueType": str(type(value)),
                      "Created": datetime.now()}

            if isinstance(value, np.ndarray):
                arr_info = dict(to_add)
                to_add["Value"] = np.array_str(value.flatten(), precision=24)
                arr_info["Key"] = str(key) + "(Reshape)"
                arr_info["Value"] = str(value.shape)
                arr_info["ValueType"] = str(type(value.shape))

                iota[database.table("Iota").insert_get_id(arr_info, sequence="IotaId")] = arr_info

            iota[database.table("Iota").insert_get_id(to_add, sequence="IotaId")] = to_add

        # update progress
        current_i += 1
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

    try:
        # create dataset
        datasetid = create_dataset(database, name, description)
        print(datasetid)
        # create the junction items
        iotadataset = []
        for iotaid in iota.keys():
            iotadataset.append({"IotaId": iotaid, "DatasetId": datasetid})

        # create IotaDatasetJuntion items
        create_iota_dataset_junction_items(database, iotadataset)
    except:
        # TODO:
        # better documentation as to why this failed
        # remove any iota dataset junction items and the dataset item that were
        # created during the try
        raise BrokenPipeError("Upload failed...")

    # return the newly added dataset row
    ds_info = get_items_in_table(database, "Dataset", {"DatasetId": datasetid})

    driver = handles.get_database_driver(database)

    if driver == "sqlite":
        ds_info = pd.DataFrame(ds_info.all())
    else:
        ds_info = pd.DataFrame([i.copy() for i in ds_info])

    return ds_info

def create_dataset(database, name, description):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(name, str)
    checks.check_types(description, [str, type(None)])

    try:
        return database.table("Dataset").insert_get_id({
            "Name": name,
            "Description": description
        }, sequence="DatasetId")
    except QueryException as e:
        print(e)
        pass

def create_iota_dataset_junction_items(database, iotadataset):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(iotadataset, list)

    # check individuals
    for iota_dataset_item in iotadataset:
        checks.check_types(iota_dataset_item, dict)

    try:
        for item in iotadataset:
           database.table("IotaDatasetJunction").insert(item)
    except QueryException as e:
        # TODO:
        # better documentation for failure to insert
        print(e)
        pass
