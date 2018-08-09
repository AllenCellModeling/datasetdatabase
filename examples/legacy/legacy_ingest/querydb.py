from orator import DatabaseManager
from modelingdbtools import query
from datetime import datetime
import pathlib
import json

serv = "local"
configs = pathlib.Path("/database/configs.json")
with open(configs) as read_in:
    configs = json.load(read_in)

# setting local store path
configs[serv][serv]["database"] = str(pathlib.Path("/database/local_store.db"))

# create database connection
db = DatabaseManager(configs[serv])
# get the dataset we just uploaded

ingestTime = datetime.now()
#test = query.get_dataset(db, id=1)


print(query.get_items_in_table(db, "IotaDatasetJunction").all())
print(query.get_items_in_table(db, "Dataset").all())

allDone = datetime.now()

print(f"query time: {allDone - ingestTime}")
