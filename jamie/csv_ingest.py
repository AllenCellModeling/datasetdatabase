import os
import re
import json
import pathlib
import shutil
import numpy as np
import pandas as pd
from orator import DatabaseManager
from modelingdbtools import query
from modelingdbtools import ingest
from modelingdbtools.utils import admin
from modelingdbtools.schemas import modeling

from datetime import datetime

start = datetime.now()

# run these uploads locally
serv = "prod_dev"
configs = pathlib.Path("/database/configs.json")
with open(configs) as read_in:
    configs = json.load(read_in)

# setting local store path
#configs[serv][serv]["database"] = str(pathlib.Path("/database/local_store.db"))

# create database connection
db = DatabaseManager(configs[serv])
# create and fill tables with basic data
modeling.create_schema(db)
modeling.add_schema_data(db)

file = r'/database/mito_annotations_only_with_pngs.csv'
df = pd.read_csv(file)

regex = r"^/root"
for i in range(len(df)):
    df.loc[i, 'inputFilename'] = os.path.normpath(os.path.join(df.loc[i, 'inputFolder'], df.loc[i, 'inputFilename']))
    df.loc[i, 'save_flat_reg_path'] = os.path.normpath(re.sub(regex, "//allen",df.loc[i, 'save_flat_reg_path']))
    outputPath = os.path.dirname(df.loc[i,'save_flat_reg_path'])
    df.loc[i, 'save_flat_proj_reg_path'] = os.path.normpath( os.path.join(outputPath, os.path.basename(df.loc[i, 'save_flat_proj_reg_path'])))
    df.loc[i, 'save_h5_reg_path'] = os.path.normpath(os.path.join(outputPath, os.path.basename(df.loc[i, 'save_h5_reg_path'])))
    # if not os.path.exists(df.loc[i, 'inputFilename']) or not os.path.isfile(df.loc[i, 'inputFilename']):
    #     print(df.loc[i, 'inputFilename'])
    # if not os.path.exists(df.loc[i, 'save_flat_reg_path']) or not os.path.isfile(df.loc[i, 'save_flat_reg_path']):
    #     print(df.loc[i, 'save_flat_reg_path'])
    # if not os.path.exists(df.loc[i, 'save_flat_proj_reg_path']) or not os.path.isfile(df.loc[i, 'save_flat_proj_reg_path']):
    #     print(df.loc[i, 'save_flat_proj_reg_path'])
#    if not os.path.exists(df.loc[i, 'save_h5_reg_path']) or not os.path.isfile(df.loc[i, 'save_h5_reg_path']):
#        print(df.loc[i, 'save_h5_reg_path'])

df.drop(columns=['inputFolder', 'save_h5_reg_path', 'targetNumeric'])

print("successful")

setup = datetime.now()

desc = ("This dataset was used to train and test the Mitosis classifiers based on Resnet18."
        "[Namely the ZProjection using the Nuclear and Membrane channel, "
        "the Majority Rule (3 resnet18) Classifier, and the Minimum Entropy (3 resnet 18) classifier.] "
        "The original data was curated by Irena in Assay Development. This does not include the ambiguous "
        "annotations that were later added with the form \"4_or_5\" as their was no determination of if it was "
        "4 or if it was 5. Ideally in the long run we will switch to a \"clock\" based model and could assign "
        "\"4_or_5\" to something like 4.5 or some other quantitative numeric value. ISODATE: 20180618\n"
        )
desc += """ Column Descriptions:
        inputFilename => CZI file used as input to gregs processing pipeline to create the PNG files
        save_flat_reg_path => Z projection of segmented cell
        save_flat_proj_reg_path => X, Y, Z, Projections in the same flat square image, [confluence modeling page -> structure] 
        outputThisCellIndex => index of cell in original CZI file
        cellID => AICS cell line Identifier
        MitosisLabel => an integer [0, 1, ..., 7] to ID the mitotic stage of cell cycle
        MitosisHandoff => is the dataset 'blessed' by AssayDev (Release)
        targetNumeric => added by Rory should be the same as MitosisLabel, but might be augmented for the ambiguous data if it's added later
        """

desc = "Mitotic Data"

ds_info = ingest.upload_dataset(database=db,
                                dataset=df,
                                name="Mitosis Classifier Dataset",
                                description=desc,
                                type_map={"inputFilename": str,
                                          "save_flat_reg_path": str,
                                          "save_flat_proj_reg_path": str,
                                          "outputThisCellIndex": int,
                                          "cellID": str,
                                          "MitosisLabel": int,
                                          "MitoticHandoff": str},
                                upload_files=False,
                                import_as_type_map=True,
                                filepath_columns=["inputFilename", "save_flat_reg_path", "save_flat_proj_reg_path"])

ingestTime = datetime.now()


# get the dataset we just uploaded

query.get_dataset(db, id=ds_info["DatasetId"][0])

allDone = datetime.now()

print(f"setup time: {setup - start}")
print(f"ingest time: {ingestTime - setup}")
print(f"query time: {allDone - ingestTime}")


#setup time: 0:00:20.250318===================================] 100.0% (7544/7544) ~ 0:00:00 remaining
#ingest time: 0:21:55.354263
#query time: 0:00:17.268981
