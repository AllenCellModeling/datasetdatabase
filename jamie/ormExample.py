import json
import pathlib
from orator import DatabaseManager
from orator import Model
from modelingdbtools.models.Iota import Iota
from modelingdbtools.models.Dataset import Dataset
from pprint import pprint

conf = pathlib.Path("/database/configs.json")
with open(conf) as read_in:
    conf = json.load(read_in)

db = DatabaseManager(conf['prod_dev'])
Model.set_connection_resolver(db)

dsets = Dataset.all()
for d in dsets:
    print(d.Name, d.Description)

iotas = Iota.where('Key', '=', 'MitosisLabel').where('Value', '=', '5').get()
for io in iotas:
    for ds in io.datasets:
        pprint(io.IotaId, ds.Name)

iotas = db.table("Iota").where("Key", "=", "MitosisLabel").where("Value", "=", "5")\
                        .join("IotaDatasetJunction", "Iota.IotaId", "=", "IotaDatasetJunction.IotaId")\
                        .join("Dataset", "Dataset.DatasetId", "=", "IotaDatasetJunction.DatasetId")\
                        .get()

print("part 2")
for io in iotas.all():
    print(io.IotaId, io.Name)
