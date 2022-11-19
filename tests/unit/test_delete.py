import pytest

from flow_sql import Delete, Query
from flow_sql.base import BaseCommand
from flow_sql.behaviours import WhereBehaviour, WithBehaviour, ReturningBehaviour, FromBehaviour
from tests.connection import open_test_connection
from tests.funcs import assert_query, create_table, insert_rows, verify_table


def test_inheritance():
    assert issubclass(Delete, BaseCommand)
    assert issubclass(Delete, WhereBehaviour)
    assert issubclass(Delete, FromBehaviour)
    assert issubclass(Delete, WithBehaviour)
    assert issubclass(Delete, ReturningBehaviour)


def test_simple():
    with open_test_connection() as conn:
        cmd = Delete(conn, from_='table1', where={'id': 1})
        assert_query(cmd, 'DELETE FROM "table1" WHERE ("id" = %(p0)s)', {'p0': 1})


def test_execute():
    with open_test_connection() as conn:
        tablename = 'table1'
        columns = {
            'id': 'int',
            'name': 'text',
        }
        create_table(conn, tablename, columns)
        data = [
            (1, 'Anna'),
            (2, 'Leo'),
            (3, 'Bruce'),
        ]
        insert_rows(conn, tablename, columns.keys(), data)
        ids_todel = [1, 3]
        Delete(conn, from_=tablename, where={'id': ids_todel}).execute()
        rows_left = [row for row in data if row[0] not in ids_todel]
        verify_table(conn, tablename, columns.keys(), rows_left)


def test_without_condition():
    with open_test_connection() as conn:
        # With no WHERE clause
        with pytest.raises(
                Exception, match='Sorry empty WHERE block is restricted on DELETE operations for security reasons'):
            Delete(conn, from_='table1').execute()

        # WHERE is explicitly None
        with pytest.raises(
                Exception, match='Sorry empty WHERE block is restricted on DELETE operations for security reasons'):
            Delete(conn, from_='table1', where=None).execute()

        # With empty id list
        cmd = Delete(conn, from_='table1', where={'id': []})
        assert_query(cmd, 'DELETE FROM "table1" WHERE (false)', {})

        # With false
        cmd = Delete(conn, from_="table1", where=False)
        assert_query(cmd, 'DELETE FROM "table1" WHERE false', {})


def test_returning():
    with open_test_connection() as conn:
        cmd = Delete(conn, from_="table1", where={'id': 1}).returning('name')
        assert_query(cmd, 'DELETE FROM "table1" WHERE ("id" = %(p0)s) RETURNING "name"', {'p0': 1})


def test_with_clause():
    with open_test_connection() as conn:
        sq1 = Query(conn).select('id').from_('table1')
        # Single WITH
        cmd = (
            Delete(conn, from_="table2", where='id IN (sq1.id)')
            .with_(sq1, alias='sq1')
        )
        assert_query(
            cmd,
            'WITH "sq1" AS (SELECT "id" FROM "table1") DELETE FROM "table2" WHERE id IN (sq1.id)',
            {}
        )

        # Two WITHes
        sq2 = Query(conn).select('id').from_('table2')
        cmd = (
            Delete(conn, from_="table3", where=True)
            .with_(sq1, 'sq1')
            .add_with(sq2, 'sq2')
        )
        assert_query(
            cmd,
            'WITH "sq1" AS (SELECT "id" FROM "table1"), "sq2" AS (SELECT "id" FROM "table2") '
            'DELETE FROM "table3" WHERE true',
            {}
        )
