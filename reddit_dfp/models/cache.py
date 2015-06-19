from r2.lib.db import tdb_cassandra
from r2.models import (
    Link,
)

class LinksByExternalId(tdb_cassandra.View):
    _use_db = True
    _connection_pool = "main"
    _read_consistency_level = tdb_cassandra.CL.ONE

    @staticmethod
    def _row_key(external_id):
        return str(external_id)

    @classmethod
    def add(cls, link):
        external_id = getattr(link, "external_id")

        cls._set_values(cls._row_key(external_id), {link._id36: ""})

    @classmethod
    def get(cls, external_id):
        try:
            columns = cls._byID(cls._row_key(external_id))._values()
            id36 = columns.keys()[0]
        except tdb_cassandra.NotFound:
            return None

        return Link._byID36(id36, data=True, return_dict=False)

