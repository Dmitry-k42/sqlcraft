from flow_sql import Command, expr, Query
from flow_sql.base import BaseCommand
from flow_sql.behaviours import ReturningBehaviour, WithBehaviour, TableBehaviour
from flow_sql.insert import Insert
from tests.connection import open_test_connection
from tests.funcs import create_table


def test_inheritance():
    assert issubclass(Insert, BaseCommand)
    assert issubclass(Insert, TableBehaviour)
    assert issubclass(Insert, WithBehaviour)
    assert issubclass(Insert, ReturningBehaviour)


def test_simple():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [
        {
            'id': 1,
            'name': 'Jane',
        },
        {
            'id': 2,
            'name': 'Ronald',
        },
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns=columns)
        Insert(conn, tablename, values=data).execute()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, name FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = [tuple(r.values()) for r in data]
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_columns_arg():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [
        (1, 'Jane'),
        (2, 'Richard'),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns=columns)
        Insert(conn, tablename, columns=columns.keys(), values=data).execute()
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = data
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_flow_iface():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'surname': 'text',
    }
    data = [
        (1, 'Jane', 'Doppler'),
        (2, 'Richard', 'Duglas'),
        (3, 'Viktor', 'Ivanov'),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        (
            Insert(conn)
            .table(tablename)
            .columns('id, name')
            .add_columns(['surname'])
            .values(data[:-1])
            .add_values(data[-1])
            .execute()
        )
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = data
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_on_conflict_do_nothing():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [
        {
            'id': 1,
            'name': 'Jamal',
        },
        {
            'id': 2,
            'name': 'Kent',
        },
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns=columns)
        Insert(conn, tablename, values=data).execute()
        Command(conn, sql_='CREATE UNIQUE INDEX idx_table1_id ON table1(id)').execute()

        new_data = {
            'id': 2,
            'name': 'Robert',
        }
        (Insert(conn, tablename, values=new_data)
            .on_conflict_do_nothing('id')
            .execute()
        )
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, name FROM {tablename}')
        actual_rows = cursor.fetchall()
        expected_rows = [tuple(r.values()) for r in data]
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_on_conflict_do_update():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'surname': 'text',
    }
    data = [
        {
            'id': 1,
            'name': 'Jamal',
            'surname': 'Carpenter',
        },
        {
            'id': 2,
            'name': 'Anna',
            'surname': 'Blant',
        },
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns=columns)
        Insert(conn, tablename, values=data).execute()
        Command(conn, sql_='CREATE UNIQUE INDEX idx_table1_id ON table1(id)').execute()

        new_data = {
            'id': 2,
            'name': 'Claudia',
            'surname': 'Richardson',
        }
        (Insert(conn, tablename, values=new_data)
            .on_conflict_do_update(
                'id',
                {
                    'surname': expr('excluded.surname'),
                },
            )
            .execute()
        )

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {tablename} WHERE id = 1')
        actual_row = cursor.fetchone()
        assert tuple(actual_row) == (1, 'Jamal', 'Carpenter')

        cursor.execute(f'SELECT * FROM {tablename} WHERE id = 2')
        actual_row = cursor.fetchone()
        assert tuple(actual_row) == (2, 'Anna', 'Richardson')


def test_select_based_insert():
    t1 = 'table1'
    t2 = 'table2'
    columns = {
        'id': 'int',
        'name': 'text',
        'surname': 'text',
    }
    data = [
        {
            'id': 1,
            'name': 'Jamal',
            'surname': 'Carpenter',
        },
        {
            'id': 2,
            'name': 'Anna',
            'surname': 'Blant',
        },
    ]
    with open_test_connection() as conn:
        create_table(conn, t1, columns)
        create_table(conn, t2, columns)
        Insert(conn, table=t1, values=data).execute()

        q = Query(conn).select(columns.keys()).from_(t1)
        Insert(conn, table=t2, values=q).execute()

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {t2}')
        actual_rows = cursor.fetchall()
        expected_rows = [tuple(r.values()) for r in data]
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_with():
    t1 = 'table1'
    t2 = 'table2'
    columns = {
        'id': 'int',
        'name': 'text',
        'surname': 'text',
    }
    data = [
        {
            'id': 1,
            'name': 'Jamal',
            'surname': 'Carpenter',
        },
        {
            'id': 2,
            'name': 'Anna',
            'surname': 'Blant',
        },
    ]
    with open_test_connection() as conn:
        create_table(conn, t1, columns)
        create_table(conn, t2, columns)
        Insert(conn, table=t1, values=data).execute()

        sq = Query(conn).select(columns.keys()).from_(t1)
        Insert(conn, table=t2, values=Query(conn).select('*').from_('sq1')).with_(sq, alias='sq1').execute()

        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {t2}')
        actual_rows = cursor.fetchall()
        expected_rows = [tuple(r.values()) for r in data]
        assert len(actual_rows) == len(expected_rows)
        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert tuple(actual_row) == expected_row


def test_returning():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
    }
    data = [(1, 'Daniel')]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        returned_id = Insert(conn, tablename, columns=columns, values=data).returning('id').scalar()
        assert returned_id == 1
