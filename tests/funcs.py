import itertools


def assert_query(cmd, expected_query, expected_params):
    actual_query = cmd.as_string()
    assert actual_query == expected_query, '"{}" != "{}"'.format(actual_query, expected_query)
    assert cmd._params == expected_params, '"{}" != "{}"'.format(cmd._params, expected_params)


def create_table(conn, tablename, columns):
    cursor = conn.cursor()
    columns_desc = ', '.join(['{} {}'.format(k, v) for k, v in columns.items()])
    cursor.execute(f"CREATE TABLE {tablename}({columns_desc})")


def drop_table(conn, tablename):
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE {tablename}')


def insert_rows(conn, tablename, columns, rows):
    col_list = ','.join(columns)
    row_list = []
    params = {}
    param_idx = 0
    for row in rows:
        row_output = []
        for item in row:
            param_name = f'p{param_idx}'
            row_output.append(f'%({param_name})s')
            params[param_name] = item
            param_idx += 1
        row_list.append('(' + ','.join(row_output) + ')')
    row_list = ','.join(row_list)
    query = f"INSERT INTO {tablename}({col_list}) VALUES{row_list}"
    cursor = conn.cursor()
    cursor.execute(query, params)


def verify_table(conn, tablename, columns, rows):
    columns = list(columns)
    # Assume that the first element in the row is the primary key
    rows = sorted(rows, key=lambda x: x[0])
    cursor = conn.cursor()
    col_desc = ','.join(columns)
    cursor.execute(f'SELECT {col_desc} FROM {tablename} ORDER BY {columns[0]}')
    db_rows = cursor.fetchall()
    comp = []
    for actual_row, expected_row in itertools.zip_longest(db_rows, rows):
        if actual_row is not None:
            actual_row = tuple(actual_row)
        if expected_row is not None:
            expected_row = tuple(expected_row)
        comp.append((actual_row, expected_row))
    ds_comp = '\n'.join(f'{actual_row}\t{expected_row}' for actual_row, expected_row in comp)
    for actual_row, expected_row in comp:
        assert actual_row == expected_row,\
            f'{actual_row} != {expected_row}\nTotal datasets:\n{ds_comp}'
