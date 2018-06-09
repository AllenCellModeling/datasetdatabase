###############################################################################
###############################################################################
## MODELING SCHEMA
###############################################################################
###############################################################################

# installed
import orator

# self
from ..utils import checks

TABLES = ["SourceType",
          "User",
          "Iota",
          "Dataset",
          "IotaDatasetJunction",
          "Run"]

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
            table.datetime("Created")
            table.string("Key", 50)
            table.string("Value")
            table.string("Parser")
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

    for table in TABLES:
        schema.drop_if_exists(table)
        print("Dropped table:", table)
