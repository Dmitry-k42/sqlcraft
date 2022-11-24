"""
Class `Connection` enhances default psycopg2 class `connection`.
You can avoid using `Coonection` class and keep using
default `connection` class with no futher issues.
Check `Connection` class docs to learn new functionality.
"""

from psycopg2.extensions import connection
from psycopg2.extras import DictRow, DictCursor


class DBRow(DictRow):
    """
    Dict-like object for accessing data fields using brackets []
    """
    def __getattr__(self, item):
        return self[item]


class DBCursor(DictCursor):
    """
    Custom DB cursor class provides dict behavior for rows.
    Example: row['field1']
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_factory = DBRow


class Connection(connection):
    """
    Connection class implements a few additional functionality:
     - Allows to access row fields by their names (like a dict object)
     - Multi-level transactions: Default psycopg2 doesn't support opening another
       transaction if you have already opened one. `Connection` class allows you to do it
    """
    def __init__(self, dsn, *more, readonly=False):
        super().__init__(dsn, *more)
        self._transaction_level = 0
        self.cursor_factory = DBCursor
        if readonly:
            self.set_session(readonly=True)

    def begin_transaction(self):
        """Start a new database transaction."""
        if self._transaction_level <= 0:
            cursor = self.cursor()
            cursor.execute('BEGIN TRANSACTION')
        else:
            point_name = self._get_point_name()
            cursor = self.cursor()
            cursor.execute('SAVEPOINT {}'.format(point_name))
        self._transaction_level += 1

    def commit(self):
        """Fix the current transaction."""
        if self._transaction_level > 0:
            self._transaction_level -= 1
        if self._transaction_level <= 0:
            super().commit()
        else:
            point_name = self._get_point_name()
            cursor = self.cursor()
            cursor.execute('RELEASE SAVEPOINT {}'.format(point_name))

    def rollback(self):
        """Rollback the current opened transaction."""
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
