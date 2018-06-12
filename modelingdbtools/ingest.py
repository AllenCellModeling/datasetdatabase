# installed
from orator.exceptions.query import QueryException
from datetime import datetime
from getpass import getuser
import pandas as pd
import pathlib
import orator

# self
from .utils import checks
from .utils import admin
from .query import *

def upload_dataset(database,
                   dataset,
                   name=None,
                   description=None,
                   type_map=None,
                   upload_files=False,
                   filepath_columns=None):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
    checks.check_types(name, [str, type(None)])
    checks.check_types(description, [str, type(None)])
    checks.check_types(type_map, [dict, type(None)])
    checks.check_types(upload_files, bool)
    checks.check_types(filepath_columns, [str, list, pd.Series, type(None)])

    # convert types
    if isinstance(dataset, str):
        dataset = pathlib.Path(dataset)

    if filepath_columns is not None:
        filepath_columns = list(filepath_columns)

    # get user
    user = getuser()

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
    checks.validate_dataset(dataset, type_map, filepath_columns)

    # actual dataset name check
    found_ds = get_items_in_table(database, "Dataset", {"Name": name})
    if len(found_ds) > 0:
        print("A dataset with that name already exists. Adding new version.")
        name += "@@" + str(datetime.now())
        sourceid = name

    # create dataset
    datasetid = create_dataset(database, name, description)

    # add user if not exists
    if len(get_items_in_table(database, "User", {"Name": user})) > 0:
        admin.add_user(database, user)

    # iter dataset and insert iota
    iota = {}
    for groupid, row in dataset.iterrows():
        # TODO:
        # handle filepaths and fms upload
        # use get userid

        r = dict(row)
        for key, value in r.items():
            to_add = {"SourceId": sourceid,
                      "SourceTypeId": sourcetypeid,
                      "GroupId": groupid,
                      "Key": str(key),
                      "Value": str(value),
                      "Created": datetime.now()}

            try:
                to_add["ValueType"] = str(type_map[key])
            except (TypeError, KeyError):
                to_add["ValueType"] = str(str)

            iota[database.table("Iota").insert_get_id(to_add)] = to_add

    # create the junction items
    iotadataset = []
    for iotaid in iota.keys():
        iotadataset.append({"IotaId": iotaid, "DatasetId": datasetid})

    # create IotaDatasetJuntion items
    create_iota_dataset_junction_items(database, iotadataset)

    # return the newly added dataset row
    ds_info = get_items_in_table(database, "Dataset", {"DatasetId": datasetid})
    return pd.DataFrame(ds_info.all())

def create_dataset(database, name, description):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(name, str)
    checks.check_types(description, [str, type(None)])

    try:
        return database.table("Dataset").insert_get_id({
            "Name": name,
            "Description": description
        })
    except QueryException:
        pass

def create_iota_dataset_junction_items(database, iotadataset):
    # check types
    checks.check_types(database, orator.DatabaseManager)
    checks.check_types(iotadataset, list)

    # check individuals
    for iota_dataset_item in iotadataset:
        checks.check_types(iota_dataset_item, dict)

    try:
        database.table("IotaDatasetJunction").insert(iotadataset)
    except QueryException as e:
        print(e)
        pass
