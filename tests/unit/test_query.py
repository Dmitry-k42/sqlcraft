from typing import Generator, Mapping

import pytest

from sqlcraft import Query, Select, Order, Alias
from sqlcraft.base import BaseCommand
from sqlcraft.behaviours import WhereBehaviour, FromBehaviour, WithBehaviour
from sqlcraft.conn import DBRow
from sqlcraft.constants import *
from tests.connection import open_test_connection
from tests.funcs import assert_query, create_table, insert_rows


def test_inheritance():
    assert issubclass(Query, BaseCommand)
    assert issubclass(Query, WhereBehaviour)
    assert issubclass(Query, FromBehaviour)
    assert issubclass(Query, WithBehaviour)


def test_query_is_select():
    assert Query is Select


def test_select():
    with open_test_connection() as conn:
        q = Query(conn)
        q.select('id')
        assert_query(q, 'SELECT "id"', {})
        q.add_select(['name as fullname', Alias('age', 'years_full'), "gender sex"])
        assert_query(q, 'SELECT "id", "name" AS "fullname", "age" AS "years_full", "gender" AS "sex"', {})
        q.select('id, name, age')
        assert_query(q, 'SELECT "id", "name", "age"', {})
        q.select('id AS code, name AS title, years(age) as "years"')
        assert_query(q, 'SELECT "id" AS "code", "name" AS "title", years(age) as "years"', {})
        q.select({'id'})
        assert_query(q, 'SELECT "id"', {})
        q.select(('id', 'name'))
        assert_query(q, 'SELECT "id", "name"', {})

        q.select(1)
        assert_query(q, 'SELECT 1', {})
        q.select(True)
        assert_query(q, 'SELECT true', {})
        q.select(False)
        assert_query(q, 'SELECT false', {})
        q.select(None)
        assert_query(q, 'SELECT NULL', {})

        sq = Query(conn).select('count(*)').from_('t2')
        q.select(Alias(ident=sq, alias='sq'))
        assert_query(q, 'SELECT (SELECT count(*) FROM "t2") AS "sq"', {})

        q.distinct().select('id')
        assert_query(q, 'SELECT DISTINCT "id"', {})
        q.distinct(True).select('id')
        assert_query(q, 'SELECT DISTINCT "id"', {})
        q.distinct(False).select('id')
        assert_query(q, 'SELECT "id"', {})


def test_alias():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1', alias='src')
        )
        assert_query(q, 'SELECT * FROM "t1" AS "src"', {})
        q.add_from('t2 src2')
        assert_query(q, 'SELECT * FROM "t1" AS "src", "t2" AS "src2"', {})
        q.from_('t1 as src')
        assert_query(q, 'SELECT * FROM "t1" AS "src"', {})
        q.from_(Alias(ident='t1', alias='src'))
        assert_query(q, 'SELECT * FROM "t1" AS "src"', {})


def test_group():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .group('id, name')
        )
        assert_query(q, 'SELECT * FROM "t1" GROUP BY "id", "name"', {})
        q.add_group(['active', 'age'])
        assert_query(q, 'SELECT * FROM "t1" GROUP BY "id", "name", "active", "age"', {})
        q.add_group('gender')
        assert_query(q, 'SELECT * FROM "t1" GROUP BY "id", "name", "active", "age", "gender"', {})
        q.group(['id', 'name'])
        assert_query(q, 'SELECT * FROM "t1" GROUP BY "id", "name"', {})


def test_order():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .order('id')
        )
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id"', {})
        q.add_order('name DESC')
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id", "name" DESC', {})
        q.add_order('age ASC')
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id", "name" DESC, "age" ASC', {})
        q.order('id asc, name desc')
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" ASC, "name" DESC', {})
        q.order(['id', 'name'])
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id", "name"', {})
        q.order(['id', ORDER_DESC])
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" DESC', {})
        q.order([['id', ORDER_DESC], ('name', ORDER_DESC)])
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" DESC, "name" DESC', {})
        q.order(Order('id', ORDER_ASC))
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" ASC', {})
        q.order([Order('id', ORDER_ASC), Order('name', ORDER_DESC)])
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" ASC, "name" DESC', {})
        q.order({'field': 'id', 'sort': ORDER_DESC})
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" DESC', {})
        q.order([{'field': 'id', 'sort': ORDER_DESC}])
        assert_query(q, 'SELECT * FROM "t1" ORDER BY "id" DESC', {})


def test_fetch():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'active': 'bool',
    }
    data = [
        (1, 'Boris', True),
        (2, 'Ali', False),
        (3, 'Paul', None),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns, data)

        q = (
            Query(conn)
            .select(['id', 'name', 'active'])
            .from_(tablename)
            .order('id')
            .limit(2)
        )
        res = q.all()
        assert isinstance(res, Generator)
        for nrow in range(2):
            row = next(res)
            assert isinstance(row, DBRow)
            assert tuple(row) == data[nrow]
        with pytest.raises(StopIteration):
            next(res)

        q = (
            Query(conn)
            .select(['id', 'name', 'active'])
            .from_(tablename)
            .order('id')
        )
        row = q.one()
        assert isinstance(row, DBRow)
        assert tuple(row) == data[0]

        q = (
            Query(conn)
            .select(['id', 'name', 'active'])
            .from_(tablename)
            .order('id')
        )
        row = q.one(as_dict=True)
        assert isinstance(row, Mapping)
        assert row == {
            'id': 1,
            'name': 'Boris',
            'active': True,
        }

        q = (
            Query(conn)
            .select(['active'])
            .from_(tablename)
            .order('id')
        )
        column = q.column(eager=True)
        assert column == [True, False, None]

        count = (
            Query(conn)
            .select('count(*)')
            .from_(tablename)
            .scalar()
        )
        assert count == 3


def test_join():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_('t2', on='t1.id=t2.id')
        )
        assert_query(q, 'SELECT * FROM "t1"  JOIN "t2" ON "t1"."id"="t2"."id"', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_('the_other_table t2', on='t1.id=t2.id')
        )
        assert_query(q, 'SELECT * FROM "t1"  JOIN "the_other_table" AS "t2" ON "t1"."id"="t2"."id"', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_('t2', on=[WHERE_OP_AND, 't1.a=t2.a', 't1.b=t2.b'])
        )
        assert_query(q, 'SELECT * FROM "t1"  JOIN "t2" ON ("t1"."a"="t2"."a") AND ("t1"."b"="t2"."b")', {})
        sq1 = Query(conn).select('*').from_('t2')
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_(sq1, alias='t2', on='t1.id=t2.id')
        )
        assert_query(
            q, 'SELECT * FROM "t1"  JOIN (SELECT * FROM "t2") AS "t2" ON "t1"."id"="t2"."id"', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_left('t2', on='t1.id=t2.id')
        )
        assert_query(q, 'SELECT * FROM "t1" LEFT JOIN "t2" ON "t1"."id"="t2"."id"', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_right('t2', on='t1.id=t2.id')
        )
        assert_query(q, 'SELECT * FROM "t1" RIGHT JOIN "t2" ON "t1"."id"="t2"."id"', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_full('t2', on='t1.id=t2.id')
        )
        assert_query(q, 'SELECT * FROM "t1" FULL JOIN "t2" ON "t1"."id"="t2"."id"', {})
        sq1 = Query(conn).select('*').from_('t2')
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .join_(sq1, alias='t2', on='t1.id=t2.id', lateral=True)
        )
        assert_query(
            q, 'SELECT * FROM "t1"  JOIN LATERAL (SELECT * FROM "t2") AS "t2" ON "t1"."id"="t2"."id"', {})


def test_subqueries():
    with open_test_connection() as conn:
        sq1 = Query(conn).select('id, name').from_('t1')
        q = (
            Query(conn)
            .select('*')
            .from_('sq')
            .with_(sq1, alias='sq')
        )
        assert_query(q, 'WITH "sq" AS (SELECT "id", "name" FROM "t1") SELECT * FROM "sq"', {})

        q = (
            Query(conn)
            .select('*')
            .from_(sq1, alias="sq")
        )
        assert_query(q, 'SELECT * FROM (SELECT "id", "name" FROM "t1") AS "sq"', {})

        sq2 = Query(conn).select('id').from_('t1')
        q = (
            Query(conn)
            .select('*')
            .from_('t2')
            .and_where([WHERE_OP_IN, 'id', sq2])
        )
        assert_query(q, 'SELECT * FROM "t2" WHERE ("id" IN (SELECT "id" FROM "t1"))', {})


def test_where_combine():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where('a')
            .and_where('b')
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE ("a") AND ("b")', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where('a')
            .or_where('b')
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE ("a") OR ("b")', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_AND, 'a', 'b'])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE ("a") AND ("b")', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_OR, 'a', 'b'])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE ("a") OR ("b")', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([
                WHERE_OP_OR,
                [WHERE_OP_AND, 'a', 'b'],
                [WHERE_OP_AND, 'c', 'd'],
            ])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE (("a") AND ("b")) OR (("c") AND ("d"))', {})


def test_where_like():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .and_where([WHERE_OP_LIKE, 'a', 'word'])
            .and_where([WHERE_OP_LIKE, 'b', 'wo%r$d'])
            .and_where([WHERE_OP_LIKE, 'c', ['word', 'word2']])
            .and_where([WHERE_OP_OR_LIKE, 'd', ['word', 'word2']])
            .and_where([WHERE_OP_NOT_LIKE, 'e', 'word'])
            .and_where([WHERE_OP_ILIKE, 'f', 'word'])
            .and_where([WHERE_OP_NOT_ILIKE, 'g', 'word'])
            .and_where([WHERE_OP_OR_NOT_ILIKE, 'h', ['w1', 'w2']])
        )
        assert_query(
            q,
            'SELECT * FROM "t1" WHERE (("a" LIKE %(p0)s))'
            ' AND (("b" LIKE %(p1)s ESCAPE \'$\'))'
            ' AND (("c" LIKE %(p2)s AND "c" LIKE %(p3)s))'
            ' AND (("d" LIKE %(p4)s OR "d" LIKE %(p5)s))'
            ' AND (("e" NOT LIKE %(p6)s))'
            ' AND (("f" ILIKE %(p7)s))'
            ' AND (("g" NOT ILIKE %(p8)s))'
            ' AND (("h" NOT ILIKE %(p9)s OR "h" NOT ILIKE %(p10)s))',
            {
                'p0': '%word%',
                'p1': '$wo$%r$$d$',
                'p2': '%word%',
                'p3': '%word2%',
                'p4': '%word%',
                'p5': '%word2%',
                'p6': '%word%',
                'p7': '%word%',
                'p8': '%word%',
                'p9': '%w1%',
                'p10': '%w2%',
            }
        )


def test_where_not():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .and_where('not a')
            .and_where([WHERE_OP_NOT, 'b'])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE (NOT "a") AND (NOT "b")', {})


def test_where_between():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .and_where([WHERE_OP_BETWEEN, 'i', 1, 2])
            .and_where([WHERE_OP_NOT_BETWEEN, 'j', 1, 2])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE ("i" BETWEEN %(p0)s AND %(p1)s)'
               ' AND ("j" NOT BETWEEN %(p2)s AND %(p3)s)',
            {'p0': 1, 'p1': 2, 'p2': 1, 'p3': 2}
        )


def test_where_lgte():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where(['=', 'a', 0.5])
            .and_where({'b': 12, 'c': 'test'})
            .and_where(['<>', 'd', 'extra'])
            .and_where(['!=', 'e', 'abc'])
            .and_where(['>', 'f', 30])
            .and_where(['>=', 'g', 31])
            .and_where(['<', 'h', 99])
            .and_where(['<=', 'i', 98])
            .and_where(['=', 'j', True])
            .and_where(['<>', 'k', True])
            .and_where(['=', 'l', False])
            .and_where(['!=', 'm', False])
            .and_where(['<>', 'n', False])
            .and_where(['=', 'o', None])
            .and_where(['!=', 'p', None])
            .and_where(['<>', 'r', None])
        )
        assert_query(
            q,
            'SELECT * FROM "t1" WHERE ("a" = %(p0)s)'
            ' AND (("b" = %(p1)s) AND ("c" = %(p2)s))'
            ' AND ("d" <> %(p3)s)'
            ' AND ("e" != %(p4)s)'
            ' AND ("f" > %(p5)s)'
            ' AND ("g" >= %(p6)s)'
            ' AND ("h" < %(p7)s)'
            ' AND ("i" <= %(p8)s)'
            ' AND ("j")'
            ' AND (NOT "k")'
            ' AND (NOT "l")'
            ' AND ("m")'
            ' AND ("n")'
            ' AND ("o" IS NULL)'
            ' AND ("p" IS NOT NULL)'
            ' AND ("r" IS NOT NULL)',
            {
                'p0': 0.5,
                'p1': 12,
                'p2': 'test',
                'p3': 'extra',
                'p4': 'abc',
                'p5': 30,
                'p6': 31,
                'p7': 99,
                'p8': 98,
            }
        )


def test_where_is_null():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where('t1.id is null')
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE "t1"."id" IS NULL', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_IS_NULL, 't1.id'])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE "t1"."id" IS NULL', {})

        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where('t1.id is not null')
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE "t1"."id" IS NOT NULL', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_IS_NOT_NULL, 't1.id'])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE "t1"."id" IS NOT NULL', {})


def test_where_exists():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_EXISTS, 1])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE EXISTS 1', {})
        sq = Query(conn).select(1)
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_EXISTS, sq])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE EXISTS (SELECT 1)', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_NOT_EXISTS, 1])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE NOT EXISTS 1', {})
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_NOT_EXISTS, sq])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE NOT EXISTS (SELECT 1)', {})


def test_where_in():
    with open_test_connection() as conn:
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where({'t1.id': set([1, 2, 3])})
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE ("t1"."id" IN %(p0)s)',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where({'t1.id': [1, 2, 3]})
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE ("t1"."id" IN %(p0)s)',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where({'t1.id': (1, 2, 3)})
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE ("t1"."id" IN %(p0)s)',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_IN, 't1.id', [1, 2, 3]])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE "t1"."id" IN %(p0)s',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where(['=', 't1.id', [1, 2, 3]])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE "t1"."id" IN %(p0)s',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_IN, 't1.id', []])
        )
        assert_query(q, 'SELECT * FROM "t1" WHERE false', {})

        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where([WHERE_OP_NOT_IN, 't1.id', [1, 2, 3]])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE "t1"."id" NOT IN %(p0)s',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where(['<>', 't1.id', [1, 2, 3]])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE "t1"."id" NOT IN %(p0)s',
            {'p0': (1, 2, 3)}
        )
        q = (
            Query(conn)
            .select('*')
            .from_('t1')
            .where(['!=', 't1.id', [1, 2, 3]])
        )
        assert_query(
            q, 'SELECT * FROM "t1" WHERE "t1"."id" NOT IN %(p0)s',
            {'p0': (1, 2, 3)}
        )


def test_nested_params():
    tablename = 'table1'
    columns = {
        'id': 'int',
        'name': 'text',
        'active': 'bool',
    }
    data = [
        (1, 'Boris', True),
        (2, 'Ali', False),
        (3, 'Paul', None),
    ]
    with open_test_connection() as conn:
        create_table(conn, tablename, columns)
        insert_rows(conn, tablename, columns, data)
        actual = (
            Query(conn)
            .select(Query(conn)
                    .select('name')
                    .from_(tablename)
                    .and_where({'id': 1})
                    )
            .scalar()
        )
        expected = 'Boris'
        assert actual == expected


def test_nested_subquery_as_value():
    with open_test_connection() as conn:
        name = 'Jane'
        q = Query(conn) \
            .select('id') \
            .from_('table1') \
            .and_where({
                'region': Query(conn).select('id2').from_('table2').and_where({'name': name})
            })
        assert_query(
            q, 'SELECT "id" FROM "table1" WHERE ("region" = (SELECT "id2" FROM "table2" WHERE ("name" = %(p0_0)s)))',
            {'p0_0': 'Jane'}
        )


def test_limit_offset():
    with open_test_connection() as conn:
        def new_q():
            return Query(conn) \
                .select('id') \
                .from_('table1')
        assert_query(new_q(), 'SELECT "id" FROM "table1"', {})
        assert_query(
            new_q().limit(15),
            'SELECT "id" FROM "table1" LIMIT 15',
            {}
        )
        assert_query(
            new_q().offset(100),
            'SELECT "id" FROM "table1" OFFSET 100',
            {}
        )
        assert_query(
            new_q().offset(100).limit(2),
            'SELECT "id" FROM "table1" LIMIT 2 OFFSET 100',
            {}
        )
        assert_query(
            new_q().limit(2).offset(100),
            'SELECT "id" FROM "table1" LIMIT 2 OFFSET 100',
            {}
        )
        assert_query(
            new_q().limit(3).limit(None),
            'SELECT "id" FROM "table1"',
            {}
        )
        assert_query(
            new_q().offset(3).offset(None),
            'SELECT "id" FROM "table1"',
            {}
        )
