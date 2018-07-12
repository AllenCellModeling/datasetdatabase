from orator import Model
from orator.orm import belongs_to_many


class Dataset(Model):
    __primary_key__ = 'DatasetId'
    __table__ = 'Dataset'

    @belongs_to_many('IotaDatasetJunction', 'DatasetId', 'IotaId')
    def iotas(self):
        from .Iota import Iota
        return Iota


