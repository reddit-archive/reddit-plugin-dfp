from r2.lib.db import tdb_cassandra
from r2.models import (
    Link,
)

class LinksByDfpCreativeId(tdb_cassandra.View):
    _use_db = True
    _connection_pool = "main"
    _read_consistency_level = tdb_cassandra.CL.ONE

    @staticmethod
    def _row_key(dfp_creative_id):
        return str(dfp_creative_id)

    @classmethod
    def add(cls, link):
        dfp_creative_id = getattr(link, "dfp_creative_id")

        cls._set_values(cls._row_key(dfp_creative_id), {link._id36: ""})

    @classmethod
    def get(cls, dfp_creative_id):
        try:
            columns = cls._byID(cls._row_key(dfp_creative_id))._values()
            id36 = columns.keys()[0]
        except tdb_cassandra.NotFound:
            return None

        return Link._byID36(id36, data=True, return_dict=False)

