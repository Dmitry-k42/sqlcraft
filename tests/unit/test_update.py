import pytest

from sqlcraft import Update
from sqlcraft.base import BaseCommand
from sqlcraft.behaviours import WhereBehaviour, TableBehaviour, WithBehaviour, ReturningBehaviour
from tests.connection import open_test_connection
from tests.funcs import create_table, insert_rows


def test_inheritance():
    assert issubclass(Update, BaseCommand)
    assert issubclass(Update, WhereBehaviour)
    assert issubclass(Update, TableBehaviour)
    assert issubclass(Update, WithBehaviour)
    assert issubclass(Update, ReturningBehaviour)


def test_simple():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [
        [1, 'Nick'],
        [2, 'Michael'],
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns.keys(), data)

        Update(conn, table=tablename, fields={'name': 'Michael Lee'}, where={'id': 2}).execute()
        data[1][1] = 'Michael Lee'

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = data
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == tuple(expected_row)


def test_flow_iface():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'age': 'int',
    }
    data = [
        [1, 'Nick', 32],
        [2, 'Michael', 56],
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns.keys(), data)

        (
            Update(conn)
            .table(tablename)
            .set({'name': 'Michael Lee'})
            .add_set('age', 57)
            .where({'id': 2})
            .execute()
        )
        data[1][1] = 'Michael Lee'
        data[1][2] = 57

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = data
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == tuple(expected_row)


def test_no_where():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [
        [1, 'Nick'],
        [2, 'Michael'],
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns.keys(), data)

        with pytest.raises(Exception) as ei:
            Update(conn, table=tablename, fields={'name': 'Michael Lee'}).execute()
        assert ei.value.args[0] == 'Sorry empty WHERE block is restricted on UPDATE operations. ' \
                                   'Please call where(True) if you have to update all rows in the table'
