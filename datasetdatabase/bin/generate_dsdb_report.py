#!/usr/bin/env python

"""
Generate a report for a DatasetDatabase that includes things like average time to pull each dataset, current size of tables, etc.
"""

# standard
import argparse
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import time

# installed
import datasetdatabase as dsdb
import numpy as np
from tqdm import tqdm


class Args(object):
    def __init__(self):
        self.__parse()

        # format
        self.config = self.config.resolve()
        self.dest_folder = self.dest_folder.resolve()

    def __parse(self):
        p = argparse.ArgumentParser(description="Generate a report for a DatasetDatabase. The report will be saved using utc-datetime.",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # Add arguments
        p.add_argument("config", type=Path,
                       help="The DSDB connection config file path for the database you want to generate a report for.")
        p.add_argument("--dest-folder", "-dest", dest="dest_folder", action="store", type=Path, default=(os.getcwd() + "/.dsdb_reports"),
                       help="The destination folder for the generated report file.")
        p.add_argument("--no-datasets", "-no-ds", dest="generate_datasets", action="store_false",
                       help="Do not create a dataset report.")
        p.add_argument("--n-datasets", "-n", dest="n_datasets", action="store", type=int, default=None,
                       help="The number of datasets to use in the report. Default is 10% of the datasets present in the database. Use '-1' for all available.")
        p.add_argument("--iterations", "-i", dest="iterations", action="store", type=int, default=3,
                       help="The number of iterations to ")
        p.add_argument("--no-database", "-no-db", dest="generate_database", action="store_false",
                       help="Do not create a database report.")
        p.add_argument("--allocated_threads", "-t", dest="threads", action="store", type=int, default=36,
                       help="Number of threads to use when communicating with the database server.")

        p.parse_args(namespace=self)


def main():
    # collect args
    args = Args()

    # create database connection
    db = dsdb.DatasetDatabase(config=args.config, processing_limit=args.threads)

    # create empty report
    report = {}

    # run datasets portion
    if args.generate_datasets:
        # create dataset portion of report
        ds_report = {}

        # get dataset list once
        # if someone uploads datasets during the run we dont want to test them
        datasets = [dict(ds) for ds in db.db.table("Dataset").where("Introspector", "=", "datasetdatabase.introspect.dataframe.DataFrameIntrospector").get()]

        # no limit given, compute limit
        if args.n_datasets is None:
            args.n_datasets = round(len(datasets) * 0.1)

        # all datasets desired, set to length
        if args.n_datasets == -1:
            args.n_datasets = len(datasets)

        # set random sample
        if args.n_datasets < len(datasets):
            datasets = random.sample(datasets, k=args.n_datasets)

        # format datasets to dict
        datasets = {ds["DatasetId"]: ds for ds in datasets}

        # if datasets fail, add them to an ignore list
        ignore_datasets = []

        # complete iterations of dataset retrieval
        for i in tqdm(range(args.iterations), desc="completed iterations"):
            for ds_id in tqdm(datasets, desc="completed datasets"):
                if ds_id not in ignore_datasets:
                    # attempt to get the dataset
                    try:
                        # start timer
                        start = time.time()

                        # get
                        with dsdb.utils.tools.suppress_prints():
                            ds = db.get_dataset(id=ds_id)

                        # compute duration
                        duration = time.time() - start

                        # create dataset iteration result
                        result = {"trial": i, "info": datasets[ds_id], "duration": duration, "iota": ds.ds.shape[0] * ds.ds.shape[1]}

                        # append to report
                        if ds_id in ds_report:
                            ds_report[ds_id].append(result)
                        else:
                            ds_report[ds_id] = [result]

                    # remove the dataset from future cycles
                    except Exception as e:
                        ignore_datasets.append(ds_id)
                        ds_report.pop(ds_id, None)

        # compute stats
        for ds in ds_report:
            durations = [trial["duration"] for trial in ds_report[ds]]

            ds_report[ds] = {
                "iota": ds_report[ds][0]["iota"],
                "average_duration": (sum(durations) / len(durations)),
                "observed_durations": durations,
                "observed_duration_variance": np.var(durations)
            }

        # generate linear fit
        weights = np.polyfit([ds_report[ds]["iota"] for ds in ds_report], [ds_report[ds]["average_duration"] for ds in ds_report], 2)

        # save predicted timings
        for ds in ds_report:
            prediction = ((((weights[0] * ds_report[ds]["iota"])**2)) + (weights[1] * ds_report[ds]["iota"]) + weights[2])
            ds_report[ds]["predicted_duration"] = prediction
            ds_report[ds]["predicted_average_duration_difference"] = math.fabs(
                (ds_report[ds]["average_duration"] - prediction) / ds_report[ds]["average_duration"]
            )

        # add ds_report to report
        report["dataset_results"] = {"datasets": ds_report, "weights": weights}

    # run database portion
    if args.generate_database:
        pass
        # print(db.constructor.tables)

    from pprint import pprint

    pprint(report)


if __name__ == "__main__":
    main()
