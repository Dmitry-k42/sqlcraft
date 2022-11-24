"""
The module includes:
 - class `Command` for custom (raw) SQL-queries
"""

from psycopg2 import sql

from .base import BaseCommand


class Command(BaseCommand):
    """
    Class Command is made for raw SQL commands execution.
    Example:
        Command(conn, 'SELECT name FROM table WHERE id=%(id)s') \
            .params({
                'id': 7,
            })
            .scalar()
    """
    def __init__(self, conn, sql_=None):
        super().__init__(conn)
        self._sql = None
        self.sql(sql_)

    def sql(self, sql_):
        """Return text representation of the SQL query."""
        self._sql = sql_
        return self

    def as_string(self):
        return self._sql

    def _on_build_query(self, ctx):
        return sql.SQL(self._sql)
