from orator import Model
from orator.orm import has_many
from .Iota import Iota


class SourceType(Model):
    __table__ = 'SourceType'
    __primary_key__ = 'SourceTypeId'

    @has_many('IotaId', 'SourceTypeId')
    def iotas(self):
        return Iota
