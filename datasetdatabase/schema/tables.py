#!/usr/bin/env python

# installed
import orator

# self
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
            table.string("Key")
            table.binary("Value")
            table.datetime("Created")
            table.unique(["Key", "Value"])


def create_Group(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Group"):
        with schema.create("Group") as table:
            table.increments("GroupId")
            table.string("MD5").unique()
            table.datetime("Created")


def create_IotaGroup(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("IotaGroup"):
        with schema.create("IotaGroup") as table:
            table.increments("IotaGroupId")
            table.integer("IotaId").unsigned()
            table.integer("GroupId").unsigned()
            table.datetime("Created")
            table.unique(["IotaId", "GroupId"])
            table.foreign("IotaId") \
                 .references("IotaId") \
                 .on("Iota")
            table.foreign("GroupId") \
                 .references("GroupId") \
                 .on("Group")


def create_Dataset(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Dataset"):
        with schema.create("Dataset") as table:
            table.increments("DatasetId")
            table.string("Name").unique()
            table.text("Description").nullable()
            table.string("Introspector")
            table.string("MD5").unique()
            table.string("SHA256").unique()
            table.datetime("Created")


def create_GroupDataset(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("GroupDataset"):
        with schema.create("GroupDataset") as table:
            table.increments("GroupDatasetId")
            table.integer("GroupId").unsigned()
            table.integer("DatasetId").unsigned()
            table.string("Label")
            table.datetime("Created")
            table.unique(["GroupId", "DatasetId", "Label"])
            table.foreign("GroupId") \
                 .references("GroupId") \
                 .on("Group")
            table.foreign("DatasetId") \
                 .references("DatasetId") \
                 .on("Dataset")


def create_Annotation(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("Annotation"):
        with schema.create("Annotation") as table:
            table.increments("AnnotationId")
            table.integer("UserId").unsigned()
            table.string("Value")
            table.datetime("Created")
            table.foreign("UserId") \
                 .references("UserId") \
                 .on("User")


def create_AnnotationDataset(schema: orator.Schema):
    # enforce types
    checks.check_types(schema, orator.Schema)

    # create table
    if not schema.has_table("AnnotationDataset"):
        with schema.create("AnnotationDataset") as table:
            table.increments("AnnotationDatasetId")
            table.integer("AnnotationId").unsigned()
            table.integer("DatasetId").unsigned()
            table.datetime("Created")
            table.foreign("AnnotationId") \
                 .references("AnnotationId") \
                 .on("Annotation")
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
            table.integer("AlgorithmParameters").unsigned()
            table.datetime("Begin")
            table.datetime("End")
            table.foreign("AlgorithmId") \
                 .references("AlgorithmId") \
                 .on("Algorithm")
            table.foreign("UserId") \
                 .references("UserId") \
                 .on("User")
            table.foreign("AlgorithmParameters") \
                 .references("DatasetId") \
                 .on("Dataset")


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
