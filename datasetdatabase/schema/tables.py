#!/usr/bin/env python

# installed
import orator

# self
from ..schema import tables
from ..utils import checks


def create_SourceType(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("SourceType"):
        with schema.create("SourceType") as table:
            table.increments("SourceTypeId")
            table.string("Type", 100).unique()
            table.string("Description").nullable()
            table.datetime("Created").nullable()


def create_User(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("User"):
        with schema.create("User") as table:
            table.increments("UserId")
            table.string("Name", 50).unique()
            table.string("Description").nullable()
            table.datetime("Created").nullable()


def create_Iota(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Iota"):
        with schema.create("Iota") as table:
            table.increments("IotaId")
            table.string("SourceId")
            table.integer("SourceTypeId")
            table.integer("GroupId")
            table.string("Key", 50)
            table.string("Value")
            table.string("ValueType")
            table.datetime("Created")
            table.unique(["SourceId", "SourceTypeId", "GroupId", "Key"])
            table.foreign("SourceTypeId") \
                 .references("SourceTypeId") \
                 .on("SourceType")


def create_Dataset(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Dataset"):
        with schema.create("Dataset") as table:
            table.increments("DatasetId")
            table.string("Name").unique()
            table.string("Description").nullable()
            table.datetime("Created")


def create_IotaDatasetJunction(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
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


def create_Algorithm(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Algorithm"):
        with schema.create("Algorithm") as table:
            table.increments("AlgorithmId")
            table.string("Name").unique()
            table.string("Source")
            table.integer("OwnerId")
            table.datetime("Created")
            table.foreign("OwnerId") \
                 .references("UserId") \
                 .on("User")


def create_Run(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Run"):
        with schema.create("Run") as table:
            table.increments("RunId")
            table.integer("InputDatasetId").nullable()
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
            table.foreign("AlgorithmId") \
                 .references("AlgorithmId") \
                 .on("Algorithm")
            table.foreign("UserId") \
                 .references("UserId") \
                 .on("User")
