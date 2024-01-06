import functools
import os
import struct
import tempfile
import threading

from psycopg2.extensions import encodings

from . import inspect
from pgcopy import util
from pgcopy.copy import array
from pgcopy.copy import BINCOPY_HEADER
from pgcopy.copy import BINCOPY_TRAILER
from pgcopy.copy import diagnostic
from pgcopy.copy import encode
from pgcopy.copy import get_formatter
from pgcopy.copy import maxsize
from pgcopy.copy import null


class CopyManager(object):
    """
    Facility for bulk-loading data using binary copy.

    Inspects the database on instantiation for the column types.

    :param conn: a database connection
    :type conn: psycopg2 connection

    :param table: the table name.  Schema may be specified using dot notation: ``schema.table``.
    :type table: str

    :param cols: columns in the table into which to copy data
    :type cols: list of str

    :raises ValueError: if the table or columns do not exist.
    """

    def __init__(self, conn, table, cols):
        self.conn = conn
        if "." in table:
            self.schema, self.table = table.split(".", 1)
        else:
            self.schema, self.table = util.get_schema(conn, table), table
        self.cols = cols
        self.compile()

    def compile(self):
        self.formatters = []
        type_dict = inspect.get_types(self.conn, self.schema, self.table)
        encoding = encodings[self.conn.encoding]
        for column in self.cols:
            att = type_dict.get(column)
            if att is None:
                message = '"%s" is not a column of table "%s"."%s"'
                raise ValueError(message % (column, self.schema, self.table))
            funcs = [encode, maxsize, array, diagnostic, null]
            reducer = lambda f, mf: mf(att, encoding, f)
            f = functools.reduce(reducer, funcs, get_formatter(att))
            self.formatters.append(f)

    def copy(self, data, fobject_factory=tempfile.TemporaryFile):
        """
        Copy data into the database using a temporary file.

        :param data: the data to be inserted
        :type data: iterable of iterables

        :param fobject_factory: a tempfile factory
        :type fobject_factory: function

        Data is serialized first in its entirety and then sent to the database.
        By default, a temporary file on disk is used.  If you have enough memory,
        you can get a slight performance benefit with in-memory storage::

            from io import BytesIO
            mgr.copy(records, BytesIO)

        For very large datasets, serialization can be done directly to the
        database connection using :meth:`threading_copy`.

        In most circumstances, however, data transfer over the network and
        db processing take significantly more time than writing and reading
        a temporary file on a local disk.

        ``ValueError`` is raised if a null value is provided for a column
        with non-null constraint.
        """
        datastream = fobject_factory()
        self.writestream(data, datastream)
        datastream.seek(0)
        self.copystream(datastream)
        datastream.close()

    def threading_copy(self, data):
        """
        Copy data, serializing directly to the database.

        :param data: the data to be inserted
        :type data: iterable of iterables
        """
        r_fd, w_fd = os.pipe()
        rstream = os.fdopen(r_fd, "rb")
        wstream = os.fdopen(w_fd, "wb")
        copy_thread = threading.Thread(target=self.copystream, args=(rstream,))
        copy_thread.start()
        self.writestream(data, wstream)
        wstream.close()
        copy_thread.join()

    def writestream(self, data, datastream):
        datastream.write(BINCOPY_HEADER)
        count = len(self.cols)
        for record in data:
            fmt = [">h"]
            rdat = [count]
            for formatter, val in zip(self.formatters, record):
                f, d = formatter(val)
                fmt.append(f)
                rdat.extend(d)
            datastream.write(struct.pack("".join(fmt), *rdat))
        datastream.write(BINCOPY_TRAILER)

    def copystream(self, datastream):
        columns = '", "'.join(self.cols)
        cmd = 'COPY "{0}"."{1}" ("{2}") FROM STDIN WITH BINARY'
        sql = cmd.format(self.schema, self.table, columns)
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(sql, datastream)
        except Exception as e:
            templ = "error doing binary copy into {0}.{1}:\n{2}"
            e.message = templ.format(self.schema, self.table, e)
            raise e
