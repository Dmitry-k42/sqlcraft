import json
from collections.abc import Mapping, Iterable
from io import StringIO

from .base import BaseCommand


class Copy:
    """
    Implements COPY command
    Examples:
        Copy(conn, 'users', ('id', 'name', 'age'), [(1, 'John', 23), (2, 'Ivan', 45)]).execute()
    """
    def __init__(self, conn, table, columns, rows, sep='\t', null='\\N'):
        self._conn = conn
        self.table = table
        self.columns = columns
        self.rows = rows
        self.sep = sep
        self.null = null

    def execute(self):
        if not self.rows:
            return
        cursor = self._conn.cursor()
        text = []
        for row in self.rows:
            text.append(self.sep.join(self._convert_value(v) for v in row))
        text = "\n".join(text)
        f = StringIO(text)
        bo = BaseCommand(self._conn)
        sql = "COPY {table}({columns}) FROM STDIN WITH DELIMITER '{sep}' NULL '{null}'".format(
            table=bo.quoted(self.table),
            columns=','.join(bo.quoted(c) for c in self.columns),
            sep=self.sep,
            null=self.null,
        )
        cursor.copy_expert(sql, f)

    def _convert_value(self, v):
        if v is None:
            return self.null
        if not isinstance(v, str) and isinstance(v, (Mapping, Iterable)):
            return json.dumps(v)
        return str(v).strip()
