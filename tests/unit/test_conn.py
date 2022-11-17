import pytest
import psycopg2
from psycopg2.extensions import connection

from flow_sql.conn import Connection, DBRow
from flow_sql.errors import OperationalError
from tests.connection import open_test_connection
from tests.funcs import create_table, drop_table, insert_rows


def test_inheritance():
    assert issubclass(Connection, connection)


def test_connect():
    with open_test_connection(connection_factory=Connection) as conn:
        assert not conn.closed
        cursor = conn.cursor()
        assert cursor is not None
        cursor.execute('SELECT now()')
        row = cursor.fetchone()
        assert row
    assert conn.closed


def test_readonly():
    with open_test_connection(connection_factory=Connection) as conn:
        assert not conn.readonly
        conn.readonly = True
        assert conn.readonly
        cursor = conn.cursor()
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            cursor.execute('DROP TABLE table1')
        conn.rollback()
        cursor = conn.cursor()
        assert cursor is not None
        cursor.execute('SELECT now()')
        row = cursor.fetchone()
        assert row


def test_transaction():
    tablename = 'table1'
    columns = {'id': 'int', 'name': 'text'}
    with open_test_connection(connection_factory=Connection) as conn:
        create_table(conn, tablename, columns)
    with open_test_connection(connection_factory=Connection) as conn:
        # Check the table has been created
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM table1')

        drop_table(conn, tablename)
        conn.rollback()

    with open_test_connection(connection_factory=Connection) as conn:
        # Check the table is still here
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM table1')

    with open_test_connection(connection_factory=Connection) as conn:
        # Delete the table for sure
        drop_table(conn, tablename)

    with open_test_connection(connection_factory=Connection) as conn:
        # Check the table is gone
        cursor = conn.cursor()
        with pytest.raises(psycopg2.errors.UndefinedTable):
            cursor.execute('SELECT * FROM table1')


def test_multilevel_transactions():
    tablename = 'table1'
    columns = {'id': 'int'}

    def get_count(conn_):
        cursor2 = conn_.cursor()
        cursor2.execute(f'SELECT count(*) FROM {tablename}')
        return cursor2.fetchone()[0]

    with open_test_connection(connection_factory=Connection) as conn:
        assert conn._transaction_level == 0
        # TL1 starts here
        conn.begin_transaction()
        assert conn._transaction_level == 1
        try:
            # TL1: Create a new table
            create_table(conn, tablename, columns)
            assert get_count(conn) == 0
            # TL2 starts here
            conn.begin_transaction()
            assert conn._transaction_level == 2
            try:
                # TL2: Insert a new row
                insert_rows(conn, tablename, columns, [(1, )])
                assert get_count(conn) == 1
                # TL3 starts here
                conn.begin_transaction()
                assert conn._transaction_level == 3
                try:
                    # TL3: Insert 2 rows more
                    insert_rows(conn, tablename, columns, [(2,), (3,)])
                    assert get_count(conn) == 3
                finally:
                    # Cancel TL3 and rollback to TL2 (only a single row in the table)
                    conn.rollback()
                    assert conn._transaction_level == 2
                assert get_count(conn) == 1
            finally:
                # TL2 commits here
                conn.commit()
                assert conn._transaction_level == 1
            # TL2: there is a row commited after TL2 in the table
            assert get_count(conn) == 1
        finally:
            # Cancel TL1. And since TL2 is part of TL1 it is going to be canceled as well
            # despite it has been commited
            conn.rollback()
            assert conn._transaction_level == 0
    with open_test_connection(connection_factory=Connection) as conn:
        # The table is not created because top-level transaction TL1 was cancelled
        cursor = conn.cursor()
        with pytest.raises(psycopg2.errors.UndefinedTable):
            cursor.execute('SELECT * FROM table1')


def test_incorrect_credentials():
    with pytest.raises(OperationalError):
        psycopg2.connect(host='*&#()&*(^#*&', database='test', user='test', password='test')


def test_fetching():
    tablename = 'table1'
    columns = {'id': 'int', 'name': 'text', 'active': 'bool'}
    data = [
        (1, 'Sally', False),
        (3, 'Richard', None),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns, data)

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename} ORDER BY id')

        row = cursor.fetchone()
        assert isinstance(row, DBRow)
        id_, name, active = row
        assert id_ == 1
        assert name == 'Sally'
        assert not active
        assert len(row) == 3
        assert row[0] == 1
        assert row[1] == 'Sally'
        assert not row[2]
        with pytest.raises(IndexError):
            row[42]
        assert row['id'] == 1
        assert row['name'] == 'Sally'
        assert not row['active']
        with pytest.raises(KeyError):
            row['unknown_field']

        row = cursor.fetchone()
        assert row['active'] is None

        row = cursor.fetchone()
        assert row is None
