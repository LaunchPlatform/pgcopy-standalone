from . import test_datatypes
from .copy_manager import CopyManager


class TestPublicSchema(test_datatypes.TypeMixin):
    tempschema = False
    datatypes = ["integer", "bool", "varchar(12)"]

    def test_default_public(self, conn, cursor, data):
        bincopy = CopyManager(conn, self.table, self.cols)
        bincopy.copy(data)
        select_list = ",".join(self.cols)
        cursor.execute("SELECT %s from public.%s" % (select_list, self.table))
        self.checkResults(cursor, data)

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v


class TestCopyFallbackSchema(test_datatypes.TypeMixin):
    datatypes = ["integer", "bool", "varchar(12)"]

    def test_fallback_schema_honors_search_path(self, conn, cursor, data, schema):
        cursor.execute("SET search_path TO {}".format(schema))
        bincopy = CopyManager(conn, self.table, self.cols)
        bincopy.copy(data)
        select_list = ",".join(self.cols)
        cursor.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults(cursor, data)

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v
