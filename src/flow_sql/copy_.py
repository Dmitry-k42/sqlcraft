"""
`Copy` class provides calling COPY queries for faster inserts to the database.
"""

import json
from collections.abc import Mapping, Iterable
from io import StringIO

from .base import BaseCommand, BuildContext


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
        """Execute the current query."""
        if not self.rows:
            return
        cursor = self._conn.cursor()
        text = []
        for row in self.rows:
            text.append(self.sep.join(self._convert_value(v) for v in row))
        text = "\n".join(text)
        fh_output = StringIO(text)
        cmd = BaseCommand(self._conn)
        ctx = BuildContext({}, {}, cmd.json_dump_fn)
        sql = "COPY {table}({columns}) FROM STDIN WITH DELIMITER '{sep}' NULL '{null}'".format(
            table=cmd.quote_string(self.table, ctx).as_string(self._conn),
            columns=','.join(cmd.quote_string(c, ctx).as_string(self._conn) for c in self.columns),
            sep=self.sep,
            null=self.null,
        )
        cursor.copy_expert(sql, fh_output)

    def _convert_value(self, value):
        if value is None:
            return self.null
        if not isinstance(value, str) and isinstance(value, (Mapping, Iterable)):
            return json.dumps(value)
        return str(value).strip()
