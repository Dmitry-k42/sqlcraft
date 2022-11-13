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
        self._sql = sql_
        return self

    def as_string(self):
        return self._sql

    def _build_query(self, param_name_prefix=None):
        super()._build_query(param_name_prefix)
        return sql.SQL(self._sql)

    def _set_param(self, value, json_stringify=False):
        raise Exception('Unimplemented here. Use `add_param` instead')
