from flow_sql import Copy
from tests.connection import open_test_connection
from tests.funcs import create_table


def test_copy():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'age': 'float',
        'active': 'bool',
    }
    data = [
        (1, 'Jane', 18, True),
        (2, 'Stella', 23.4, None),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        Copy(conn, tablename, columns.keys(), data).execute()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, name, age, active FROM {tablename}')
        actual_rows = cursor.fetchall()
        assert len(actual_rows) == len(data)
        for actual_row, expected_row in zip(actual_rows, data):
            assert tuple(actual_row) == tuple(expected_row)


def test_json():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'info': 'jsonb',
    }
    data = [
        (1, {'status': 'ok', 'ids': [1, 2, 3], 'payload': {'progress': 99.7, 'unit': 'percents', 'bool': True}})
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        Copy(conn, tablename, columns.keys(), data).execute()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, info FROM {tablename}')
        actual_row = cursor.fetchone()
        expected_row = data[0]
        assert actual_row[0] == expected_row[0]
        actual_info = actual_row[1]
        expected_info = data[0][1]
        assert actual_info == expected_info
