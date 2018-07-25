#!/usr/bin/env python

# installed
import orator

# self
from ..schema import tables
from ..utils import checks


def create_User(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("User"):
        with schema.create("User") as table:
            table.increments("UserId")
            table.string("Name", 50).unique()
            table.string("Description").nullable()
            table.datetime("Created")


def create_Iota(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Iota"):
        with schema.create("Iota") as table:
            table.big_increments("IotaId")
            table.integer("GroupId")
            table.string("Key", 50)
            table.string("Value")
            table.string("ValueType")
            table.datetime("Created")
            table.unique(["GroupId", "Key", "Value", "ValueType"])


def create_Source(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Source"):
        with schema.create("Source") as table:
            table.increments("SourceId")
            table.datetime("Created")


def create_FileSource(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("FileSource"):
        with schema.create("FileSource") as table:
            table.increments("FileSourceId")
            table.integer("FileId").unsigned()
            table.integer("SourceId").unsigned()
            table.foreign("SourceId") \
                 .references("SourceId") \
                 .on("Source")


def create_QuiltSource(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("QuiltSource"):
        with schema.create("QuiltSource") as table:
            table.increments("QuiltSourceId")
            table.string("PackageString")
            table.integer("SourceId").unsigned()
            table.foreign("SourceId") \
                 .references("SourceId") \
                 .on("Source")


def create_Dataset(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Dataset"):
        with schema.create("Dataset") as table:
            table.increments("DatasetId")
            table.string("Name").unique()
            table.string("Description").nullable()
            table.integer("SourceId").unsigned()
            table.string("FilepathColumns").nullable()
            table.datetime("Created")
            table.foreign("SourceId") \
                 .references("SourceId") \
                 .on("Source")


def create_IotaDatasetJunction(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("IotaDatasetJunction"):
        with schema.create("IotaDatasetJunction") as table:
            table.increments("IotaDatasetJunctionId")
            table.integer("IotaId").unsigned()
            table.integer("DatasetId").unsigned()
            table.datetime("Created")
            table.unique(["IotaId", "DatasetId"])
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
            table.string("Name")
            table.string("Description").nullable()
            table.string("Version")
            table.datetime("Created")
            table.unique(["Name", "Version"])


def create_Run(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Run"):
        with schema.create("Run") as table:
            table.increments("RunId")
            table.integer("AlgorithmId").unsigned()
            table.integer("UserId").unsigned()
            table.string("Name").nullable()
            table.string("Description").nullable()
            table.datetime("Begin")
            table.datetime("End")
            table.foreign("AlgorithmId") \
                 .references("AlgorithmId") \
                 .on("Algorithm")
            table.foreign("UserId") \
                 .references("UserId") \
                 .on("User")


def create_RunInput(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("RunInput"):
        with schema.create("RunInput") as table:
            table.increments("RunInputId")
            table.integer("RunId").unsigned()
            table.integer("DatasetId").unsigned()
            table.datetime("Created")
            table.foreign("RunId") \
                 .references("RunId") \
                 .on("Run")
            table.foreign("DatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")


def create_RunOutput(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("RunOutput"):
        with schema.create("RunOutput") as table:
            table.increments("RunOutputId")
            table.integer("RunId").unsigned()
            table.integer("DatasetId").unsigned()
            table.datetime("Created")
            table.foreign("RunId") \
                 .references("RunId") \
                 .on("Run")
            table.foreign("DatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")


def create_RunSource(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("RunSource"):
        with schema.create("RunSource") as table:
            table.increments("RunSourceId")
            table.integer("SourceId").unsigned()
            table.integer("RunId").unsigned()
            table.foreign("SourceId") \
                 .references("SourceId") \
                 .on("Source")
            table.foreign("RunId") \
                 .references("RunId") \
                 .on("Run")
