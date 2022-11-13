from psycopg2.extensions import connection
from psycopg2.extras import DictRow, DictCursor


class DBRow(DictRow):
    def __getattr__(self, item):
        return self[item]


class DBCursor(DictCursor):
    def __init__(self, *args, **kwargs):
        super(DBCursor, self).__init__(*args, **kwargs)
        self.row_factory = DBRow


class Connection(connection):
    def __init__(self, dsn, *more, readonly=False):
        super(Connection, self).__init__(dsn, *more)
        self._transaction_level = 0
        self.cursor_factory = DBCursor
        if readonly:
            self.set_session(readonly=True)

    def begin_transaction(self):
        if self._transaction_level <= 0:
            cursor = self.cursor()
            cursor.execute('BEGIN TRANSACTION')
        else:
            point_name = self._get_point_name()
            cursor = self.cursor()
            cursor.execute('SAVEPOINT {}'.format(point_name))
        self._transaction_level += 1

    def commit(self):
        if self._transaction_level > 0:
            self._transaction_level -= 1
        if self._transaction_level <= 0:
            super().commit()
        else:
            point_name = self._get_point_name()
            cursor = self.cursor()
            cursor.execute('RELEASE SAVEPOINT {}'.format(point_name))

    def rollback(self):
        if self._transaction_level > 0:
            self._transaction_level -= 1
        if self._transaction_level <= 0:
            super().rollback()
        else:
            point_name = self._get_point_name()
            cursor = self.cursor()
            cursor.execute('ROLLBACK TO SAVEPOINT {}'.format(point_name))

    def _get_point_name(self):
        return 'sp_{}'.format(self._transaction_level)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.close()

    def __del__(self):
        self.close()
