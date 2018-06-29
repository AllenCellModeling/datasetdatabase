from orator import Model
from orator.orm import belongs_to_many


class Iota(Model):
    __table__ = 'Iota'
    __primary_key__ = 'IotaId'

    @belongs_to_many('IotaDatasetJunction', 'IotaId', 'DatasetId')
    def datasets(self):
        from .Dataset import Dataset
        return Dataset

