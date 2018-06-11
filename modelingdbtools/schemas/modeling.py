###############################################################################
###############################################################################
## MODELING SCHEMA
###############################################################################
###############################################################################

# installed
import orator
from datetime import datetime

# self
from ..utils import checks

def create_schema(database):
    checks.check_types(database, orator.DatabaseManager)

    schema = orator.Schema(database)

    if not schema.has_table("SourceType"):
        with schema.create("SourceType") as table:
            table.increments("SourceTypeId")
            table.string("Type", 100).unique()
            table.string("Description").nullable()

        print("Created table: SourceType")

    if not schema.has_table("User"):
        with schema.create("User") as table:
            table.increments("UserId")
            table.string("Name", 50).unique()
            table.string("Description").nullable()
            table.datetime("Created")

        print("Created table: User")

    if not schema.has_table("Iota"):
        with schema.create("Iota") as table:
            table.increments("IotaId")
            table.integer("SourceId")
            table.integer("SourceTypeId")
            table.integer("GroupId")
            table.string("Key", 50)
            table.string("Value")
            table.string("Parser")
            table.datetime("Created")
            table.unique(["SourceId", "SourceType", "GroupId", "Key"])
            table.foreign("SourceTypeId") \
                 .references("SourceTypeId") \
                 .on("SourceType")

        print("Created table: Iota")

    if not schema.has_table("Dataset"):
        with schema.create("Dataset") as table:
            table.increments("DatasetId")
            table.string("Name").unique()
            table.string("Description").nullable()

        print("Created table: Dataset")

    if not schema.has_table("IotaDatasetJunction"):
        with schema.create("IotaDatasetJunction") as table:
            table.integer("IotaId")
            table.integer("DatasetId")
            table.primary(["IotaId", "DatasetId"])
            table.foreign("IotaId") \
                 .references("IotaId") \
                 .on("Iota")
            table.foreign("DatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")

        print("Created table: IotaDatasetJunction")

    if not schema.has_table("Run"):
        with schema.create("Run") as table:
            table.increments("RunId")
            table.integer("InputDatasetId")
            table.integer("OutputDatasetId")
            table.integer("AlgorithmId")
            table.integer("UserId")
            table.string("Name").nullable()
            table.string("Description").nullable()
            table.datetime("Begin")
            table.datetime("End")
            table.foreign("InputDatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")
            table.foreign("OutputDatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")
            table.foreign("UserId") \
                 .references("UserId") \
                 .on("User")

        print("Created table: Run")

def drop_schema(database):
    checks.check_types(database, orator.DatabaseManager)

    schema = orator.Schema(database)

    table_order = ["Run",
                   "IotaDatasetJunction",
                   "Dataset",
                   "Iota",
                   "User",
                   "SourceType"]

    for table in table_order:
        schema.drop_if_exists(table)
        print("Dropped table:", table)

def add_schema_testing_data(database):
    checks.check_types(database, orator.DatabaseManager)

    try:
        database.table("SourceType").insert([
            {"Type": "File",
             "Description": "Use aics.FMS.File id to find source"},
            {"Type": "Dataset",
             "Description": "Use aics.Modeling.Dataset id to find source"}
        ])
    except orator.QueryException:
        pass

    try:
        database.table("User").insert([
            {"Name": "jacksonb",
             "Description": "admin",
             "Created": datetime.now()},
            {"Name": "gregj",
             "Description": "scrub, noob, etc",
             "Created": datetime.now()}
        ])
    except orator.QueryException as e:
        pass

    try:
        database.table("Iota").insert([
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "hello",
             "Value": "world1",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "foo",
             "Value": "bar1",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "is_test",
             "Value": "True",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 1,
             "Created": datetime.now(),
             "Key": "hello",
             "Value": "world2",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 1,
             "Created": datetime.now(),
             "Key": "foo",
             "Value": "bar2",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 1,
             "Created": datetime.now(),
             "Key": "is_test",
             "Value": "False",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "dataset_created",
             "Value": "True",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "passed_run",
             "Value": "True",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "origin_dataset_id",
             "Value": "1",
             "Parser": "int"}
        ])
    except orator.QueryException:
        pass

    try:
        database.table("Dataset").insert([
            {"Name": "test_input_dataset",
             "Description": "Created by queries.add_filler_data"},
            {"Name": "test_output_dataset",
             "Description": "Created by queries.add_filler_data"}
        ])
    except orator.QueryException:
        pass

    try:
        database.table("IotaDatasetJunction").insert([
            {"IotaId": 1,
             "DatasetId": 1},
            {"IotaId": 2,
             "DatasetId": 1},
            {"IotaId": 3,
             "DatasetId": 1},
            {"IotaId": 4,
             "DatasetId": 1},
            {"IotaId": 5,
             "DatasetId": 1},
            {"IotaId": 6,
             "DatasetId": 1},
            {"IotaId": 7,
             "DatasetId": 2},
            {"IotaId": 8,
             "DatasetId": 2},
            {"IotaId": 9,
             "DatasetId": 2}
        ])
    except orator.QueryException:
        pass

    try:
        database.table("Run").insert([
            {"InputDatasetId": 1,
             "OutputDatasetId": 2,
             "AlgorithmId": 1,
             "UserId": 1,
             "Name": "Test Run",
             "Description": "Created by queries.add_filler_data",
             "Begin": datetime.now(),
             "End": datetime.now()}
        ])
    except orator.QueryException:
        pass
