from collections.abc import Sized, Mapping, Sequence, Iterable

from psycopg2 import sql

from constants import *
from misc import alias, where_cond, where_cond_arr, where_cond_raw, with_subquery
from base import BaseCommand


class FromBehaviour:
    def __init__(self, ob: BaseCommand):
        self._from = []
        self._o = ob

    def from_(self, table, alias=None):
        """
        Sets FROM query block
        Usage examples:
        * Simple usage:
            from_('auth.users') => FROM "auth"."users"
        * Table alias:
            from_('auth.users', 'u') => FROM "auth"."users" AS "u"
            from_('auth.users', alias='u') => FROM "auth"."users" AS "u"
        * Alias short notation:
            from_('auth.users AS u') => FROM "auth"."users" AS "u"
            from_('auth.users u') => FROM "auth"."users" AS "u"
        * With special type `alias`
            from_(alias(ident='auth.users', alias='u'))
                => FROM "auth"."users" AS "u"
        * Subqueries supported:
            subq = Query(conn).select(*).from('cars')
            from(subq, alias='c')
                => FROM (SELECT * FROM "cars") AS "c"
        :param table:
        :param alias:
        :return:
        """
        self._from = []
        return self.add_from(table, alias)

    def add_from(self, table, alias=None):
        self._from.append(self._o._parse_column_or_table(table, alias))
        return self

    def _build_query_from(self):
        if not self._from:
            return None
        res = sql.SQL('FROM ') + sql.SQL(', ').join(self._o._quote_table(x) for x in self._from)
        return res


class WithBehaviour:
    def __init__(self, ob: BaseCommand):
        self._with = []
        self._o = ob

    def with_(self, subquery, alias, recursive=False):
        """
        Sets WITH query block
        Usage examples:
        * Simple string:
            with_('SELECT 1, 2, 3', alias='cte')
                => WITH "cte" AS (SELECT 1, 2, 3)
        * Subqueries supported:
            subq = Query(conn).select(['id', 'name']).from_('users')
            with_(subq, alias='u')
                => WITH "u" AS (SELECT "id", "name" FROM "users")
        :param subquery: string or Query
        :param alias: alias for the subquery
        :param recursive: whether this subquery is recursive
        :return: self
        """
        self._with = []
        self.add_with(subquery, alias, recursive)
        return self

    def add_with(self, subquery, alias_, recursive=False):
        """
        Adds a new subquery to WITH query block. See example on `with_` method
        :param subquery: string or Query
        :param alias_: alias for the subquery
        :param recursive: whether this subquery is recursive
        :return: self
        """
        self._with.append(with_subquery(alias(ident=subquery, alias=alias_), recursive))
        return self

    def _build_query_with(self):
        if len(self._with) == 0:
            return None
        subqueries = []
        for sub in self._with:
            ident = sub.subquery.ident
            if isinstance(ident, BaseCommand):
                query = self._o._build_subquery(ident)
            else:
                query = sql.SQL(ident)
            subqueries.append(sql.SQL('{recursive}{alias} AS ({query})').format(
                recursive=sql.SQL('RECURSIVE ' if sub.recursive else ''),
                alias=self._o._quote_string(sub.subquery.alias),
                query=query,
            ))
        return sql.SQL('WITH ') + (sql.SQL(', ').join(subqueries))


class TableBehaviour:
    def __init__(self, ob: BaseCommand, table=None, alias=None):
        self._o = ob
        self._table = None
        if table is not None:
            self.table(table, alias)

    def table(self, table, alias=None):
        """
        Sets the table into the query.
        Usage examples:
        * Simple text:
            table('users') => "users"
            table('users u') => "users" AS "u"
        * Separated alias:
            table('users', alias='u') => "users" AS "u"
        :param table: str
        :param alias: str. Can be ommited
        :return: self
        """
        self._table = self._o._parse_column_or_table(table, alias)
        return self


class ReturningBehaviour:
    def __init__(self, ob: BaseCommand):
        self._o = ob
        self._returning = []

    def returning(self, fields):
        """
        Sets RETURNING query block. `fields` parameter maybe string or iterable object
        Usage examples:
        * Comma-separated fields:
            returning('id, name, u.address')
                => RETURNING "id", "name", "u"."address"
        * List of fields:
            returning(['id', 'name', 'u.address'])
                => RETURNING "id", "name", "u"."address"
        * Tuple:
            returning(('id', 'name')) => RETURNING "id", "name"
        * Set:
            returning({'id', 'name'}) => RETURNING "id", "name"
        * All fields:
            returning('*') => RETURNING *
        * Expressions: If the field contains "(" or ")" symbol then it will not be quoted
            returning("concat(first_name, ' ', last_name)") => RETURNING concat(first_name, ' ', last_name)
        * AS-syntax:
            returning('c.name AS city_name') => RETURNING "c"."name" AS "city_name"
            returning('c.name city_name') => RETURNING "c"."name" AS "city_name"
        * Subquery:
            subq = Query(conn).select(1)
            returning(subq)
                => RETURNING (SELECT 1)
        :param fields: the fields to add
        :return: self
        """
        self._returning = []
        self.add_returning(fields)
        return self

    def add_returning(self, fields):
        """
        Appends new fields to RETURNING block. See `returning` method
        :param fields: the fields to add
        :return: self
        """
        if isinstance(fields, alias):
            self._returning.append(fields)
            return self
        if isinstance(fields, str):
            fields = fields.split(',')
        elif not isinstance(fields, Iterable):
            fields = [fields]
        for field in fields:
            if isinstance(field, str):
                field = self._o._parse_column_or_table(field.strip())
            self._returning.append(field)
        return self

    def _build_query_returning(self):
        if len(self._returning) == 0:
            return None
        return sql.SQL('RETURNING ') + (sql.SQL(', ').join([
            self._o._quote_column(field) for field in self._returning
        ]))


class WhereBehaviour:
    def __init__(self, ob: BaseCommand):
        self._where = None
        self._o = ob

    def where(self, cond):
        """
        Sets WHERE query block.
        `cond` parameter describes new adding conditions. See some examples below.
        Usage examples:
        * Constants:
            where(True) => WHERE true
            where(None) => WHERE NULL
            where(-12.5) => WHERE -12.5
        * Strings:
            where('u.id=a.user_id') => WHERE "u"."id"="a"."user_id"
        * Strings with "(" or ")" symbols will not be quoted:
            where('count(*) > 14') => WHERE count(*) > 14
        * Equal conditions:
            where({'id': 12, 'name': 'John Doe', 'age': [12, 13, 14]})
                => WHERE ("id"=12) AND ("name"='John Doe') AND ("age" IN (12, 13, 14))
            where(['id', 12])
                => WHERE "id"=12
        * Operator conditions. There are supported several operators (see WHERE_OP_* constants):
            where(['<=', 'u.age', 18]) => WHERE "u"."age" <= 18
            where(['BETWEEN', 'u.age', 18, 32])
                => WHERE "u"."age" BETWEEN 18 AND 32
        * Operator conditions with `where_cond`:
            where(where_cond(op=WHERE_OP_OR_ILIKE, ident="u.name", value=['john', 'james']) =>
                WHERE "u"."name" ILIKE '%john%' OR "u"."name" ILIKE '%james%'
        * Conjuctions:
            where([
                'and',
                ('in', 'u.fullname', ['Ivan', 'Richard']),
                (WHERE_OP_GTE, 'u.age', 18),
            ])
                => WHERE ("u"."fullname" IN ('Ivan', 'Richard')) AND ("u"."age" >= 18)
        * Conjuctions with `where_cond_arr`:
            where(where_cond_arr(op=WHERE_OP_OR, conds=[('u.name', 'John Doe'), ('u.email', 'user@example.com')]
                => WHERE ("u"."name" = 'John Doe') OR ("u"."email" = 'user@example.com')
        * Subqueries are supported. For example:
            subq = Query(conn).select(1).from('users')
            where([WHERE_OP_EXISTS, subq])
                => WHERE EXISTS (SELECT 1 FROM "users")
            subq = Query(conn).select('count(*)').from('cars c').where('c.user_id=u.id')
            where(['>=', subq, 2])
                => WHERE (SELECT count(*) FROM "cars" AS "c" WHERE "c"."user_id"="u"."id") >= 2
        :param cond: the condition for the WHERE block
        :return: self
        """
        self._where = self._parse_where_cond(cond)
        return self

    def and_where(self, cond):
        """
        Adds a new condition to the WHERE query block with AND conjunction.
        See `where` method for examples
        :param cond: a new condition.
        :return: self
        """
        self._where_arr(cond, WHERE_OP_AND)
        return self

    def or_where(self, cond):
        """
        Adds a new condition to the WHERE query block with OR conjunction.
        See `where` method for examples
        :param cond: a new condition.
        :return: self
        """
        self._where_arr(cond, WHERE_OP_OR)
        return self

    def _where_arr(self, cond, cond_op):
        cond_parsed = self._parse_where_cond(cond)
        if isinstance(self._where, where_cond_arr) and self._where.op == cond_op:
            if isinstance(cond_parsed, where_cond_arr) and cond_parsed.op == cond_op:
                self._where.conds.extend(cond_parsed.conds)
            else:
                self._where.conds.append(cond_parsed)
        elif isinstance(cond_parsed, where_cond_arr) and cond_parsed.op == cond_op and self._where is None:
            self._where = cond_parsed
        else:
            conds = []
            if self._where:
                conds.append(self._where)
            conds.append(cond_parsed)
            self._where = where_cond_arr(op=cond_op, conds=conds)
        return self

    def _parse_where_cond(self, cond):
        if isinstance(cond, where_cond):
            return cond
        if isinstance(cond, where_cond_arr):
            return cond
        if type(cond) in [bool, float, int]:
            return where_cond_raw(value=cond)
        if isinstance(cond, str):
            known_suffixes = {
                ' is null': (WHERE_OP_IS_NULL, None),
                ' is not null': (WHERE_OP_IS_NOT_NULL, None),
            }
            for suffix, (op, value) in known_suffixes.items():
                if cond.lower().endswith(suffix):
                    return where_cond(op, cond[:-len(suffix)], value)
            known_prefixes = {
                'not ': (WHERE_OP_NOT, None),
            }
            for prefix, (op, value) in known_prefixes.items():
                if cond.lower().startswith(prefix):
                    return where_cond(op, cond[len(prefix):], value)
            return where_cond_raw(value=cond)
        elif not isinstance(cond, str) and isinstance(cond, Sized) and isinstance(cond, Sequence):
            op = cond[0].lower()
            if op in WHERE_COMP_OPS:
                return where_cond_arr(op, [self._parse_where_cond(x) for x in cond[1:]])
            if op in self._parse_where_map:
                # noinspection PyArgumentList
                return self._parse_where_map[op](self, cond)
            if len(cond) == 2:
                op2 = WHERE_OP_IN if isinstance(cond[1], Sequence) and not isinstance(cond[1], str) else WHERE_OP_EQUAL
                return where_cond(op2, cond[0], cond[1])
        elif isinstance(cond, Mapping):
            conds = []
            for ident, value in cond.items():
                if isinstance(value, BaseCommand):
                    op2 = WHERE_OP_EQUAL
                elif isinstance(value, Sequence) and not isinstance(value, str):
                    op2 = WHERE_OP_IN
                elif value is None:
                    op2 = WHERE_OP_IS_NULL
                else:
                    op2 = WHERE_OP_EQUAL
                conds.append(where_cond(op2, ident, value))
            return where_cond_arr(WHERE_OP_AND, conds)
        elif isinstance(cond, sql.Composable):
            return where_cond_raw(value=cond)
        raise Exception('Unknown format of the where condition')

    def _parse_where_0_param(self, cond):
        return where_cond(op=cond[0].lower(), ident=cond[1], value=None)

    def _parse_where_1_param(self, cond):
        return where_cond(op=cond[0].lower(), ident=cond[1], value=cond[2])

    def _parse_where_between(self, cond):
        return where_cond(op=cond[0].lower(), ident=cond[1], value=(cond[2], cond[3]) if len(cond) == 4 else cond[2])

    _parse_where_map = {
        WHERE_OP_EQUAL: _parse_where_1_param,
        WHERE_OP_LT: _parse_where_1_param,
        WHERE_OP_LTE: _parse_where_1_param,
        WHERE_OP_GT: _parse_where_1_param,
        WHERE_OP_GTE: _parse_where_1_param,
        WHERE_OP_NE: _parse_where_1_param,
        WHERE_OP_NE2: _parse_where_1_param,
        WHERE_OP_IN: _parse_where_1_param,
        WHERE_OP_NOT_IN: _parse_where_1_param,
        WHERE_OP_LIKE: _parse_where_1_param,
        WHERE_OP_ILIKE: _parse_where_1_param,
        WHERE_OP_NOT_LIKE: _parse_where_1_param,
        WHERE_OP_NOT_ILIKE: _parse_where_1_param,
        WHERE_OP_OR_LIKE: _parse_where_1_param,
        WHERE_OP_OR_ILIKE: _parse_where_1_param,
        WHERE_OP_OR_NOT_LIKE: _parse_where_1_param,
        WHERE_OP_OR_NOT_ILIKE: _parse_where_1_param,
        WHERE_OP_NOT: _parse_where_0_param,
        WHERE_OP_BETWEEN: _parse_where_between,
        WHERE_OP_NOT_BETWEEN: _parse_where_between,
        WHERE_OP_EXISTS: _parse_where_0_param,
        WHERE_OP_NOT_EXISTS: _parse_where_0_param,
        WHERE_OP_IS_NULL: _parse_where_0_param,
        WHERE_OP_IS_NOT_NULL: _parse_where_0_param,
    }
    assert sorted(_parse_where_map.keys()) == sorted(WHERE_OPS)

    def _build_query_where(self):
        if self._where is None:
            return None
        res = self._build_query_where_iter(self._where)
        if res is None:
            return None
        return sql.SQL('WHERE ') + res

    def _build_query_where_iter(self, cond):
        if isinstance(cond, where_cond_arr):
            conds = [self._build_query_where_iter(x) for x in cond.conds]
            conds = [x for x in conds if x is not None]
            if len(conds) == 0:
                return None
            glue = sql.SQL(' %s ' % cond.op.upper())
            return glue.join([sql.SQL('(') + x + sql.SQL(')') for x in conds])
        elif isinstance(cond, where_cond):
            method = self._build_query_where_map[cond.op] if cond.op in self._build_query_where_map else None
            if method is None:
                raise Exception('Unknown where operator "%s"' % cond.op)
            # noinspection PyArgumentList
            return method(self, cond)
        elif isinstance(cond, where_cond_raw):
            if isinstance(cond.value, str):
                v = self._o._quote_string(cond.value)
            elif isinstance(cond.value, sql.Composable):
                v = cond.value
            else:
                v = sql.Literal(cond.value)
            return v
        return None

    def _build_query_where_lgte(self, cond: where_cond):
        value = cond.value
        if isinstance(value, BaseCommand):
            placeholder = sql.SQL('(') + self._o._build_subquery(value) + sql.SQL(')')
        else:
            placeholder = self._o._set_param(value)
        return sql.SQL(' %s ' % cond.op).join([
            self._o._quote_string(cond.ident),
            placeholder,
        ])

    def _build_query_where_is_null(self, cond: where_cond):
        return sql.SQL(' ').join([
            self._o._quote_string(cond.ident),
            sql.SQL(cond.op.upper()),
        ])

    def _build_query_where_in(self, cond: where_cond):
        value = cond.value
        if isinstance(value, BaseCommand):
            placeholder = sql.SQL('(') + self._o._build_subquery(value) + sql.SQL(')')
        else:
            if not isinstance(value, tuple):
                value = tuple(value) if not isinstance(value, str) else tuple([value])
            # In case of empty value in order to prevent syntax error we replace condition 'in ()' with 'false'
            # That is behaviour which the users expects
            if len(value) == 0:
                return sql.SQL('false')
            placeholder = self._o._set_param(value)
        return sql.SQL(' %s ' % cond.op.upper()).join([
            self._o._quote_string(cond.ident),
            placeholder,
        ])

    def _build_query_where_like(self, cond: where_cond):
        values = cond.value
        if isinstance(values, str):
            values = [values]
        comp_op = WHERE_OP_AND
        op = cond.op
        if op == WHERE_OP_OR_LIKE:
            comp_op = WHERE_OP_OR
            op = WHERE_OP_LIKE
        elif op == WHERE_OP_OR_ILIKE:
            comp_op = WHERE_OP_OR
            op = WHERE_OP_ILIKE
        elif op == WHERE_OP_OR_NOT_LIKE:
            comp_op = WHERE_OP_OR
            op = WHERE_OP_NOT_LIKE
        elif op == WHERE_OP_OR_NOT_ILIKE:
            comp_op = WHERE_OP_OR
            op = WHERE_OP_NOT_ILIKE
        comp_op = comp_op.upper()
        op = op.upper()
        conds = []
        for value in values:
            escaping = sql.SQL('')
            if '%' in value:
                value = value.replace('$', '$$').replace('%', '$%')
                escaping = sql.SQL(" ESCAPE '$'")
            conds.append(sql.SQL("{ident} {op} {value}{escaping}").format(
                ident=self._o._quote_string(cond.ident),
                op=sql.SQL(op.upper()),
                value=self._o._set_param('%{}%'.format(value)),
                escaping=escaping,
            ))
        return sql.SQL('(') + sql.SQL(' %s ' % comp_op).join(conds) + sql.SQL(')') if len(conds) > 0 else None

    def _build_query_where_not(self, cond: where_cond):
        return sql.SQL('%s ' % cond.op.upper()) + self._o._quote_string(cond.ident)

    def _build_query_where_between(self, cond: where_cond):
        lower, upper = cond.value
        param1 = self._o._set_param(lower)
        param2 = self._o._set_param(upper)
        return self._o._quote_string(cond.ident) + sql.SQL(' {op} {lower} AND {upper}').format(
            op=sql.SQL(cond.op.upper()),
            lower=param1,
            upper=param2,
        )

    def _build_query_where_exists(self, cond: where_cond):
        return sql.SQL('%s ' % cond.op.upper()) + self._o._quote_string(cond.ident)

    _build_query_where_map = {
        WHERE_OP_EQUAL: _build_query_where_lgte,
        WHERE_OP_LT: _build_query_where_lgte,
        WHERE_OP_LTE: _build_query_where_lgte,
        WHERE_OP_GT: _build_query_where_lgte,
        WHERE_OP_GTE: _build_query_where_lgte,
        WHERE_OP_NE: _build_query_where_lgte,
        WHERE_OP_NE2: _build_query_where_lgte,
        WHERE_OP_IN: _build_query_where_in,
        WHERE_OP_NOT_IN: _build_query_where_in,
        WHERE_OP_LIKE: _build_query_where_like,
        WHERE_OP_ILIKE: _build_query_where_like,
        WHERE_OP_NOT_LIKE: _build_query_where_like,
        WHERE_OP_NOT_ILIKE: _build_query_where_like,
        WHERE_OP_OR_LIKE: _build_query_where_like,
        WHERE_OP_OR_ILIKE: _build_query_where_like,
        WHERE_OP_OR_NOT_LIKE: _build_query_where_like,
        WHERE_OP_OR_NOT_ILIKE: _build_query_where_like,
        WHERE_OP_NOT: _build_query_where_not,
        WHERE_OP_BETWEEN: _build_query_where_between,
        WHERE_OP_NOT_BETWEEN: _build_query_where_between,
        WHERE_OP_EXISTS: _build_query_where_exists,
        WHERE_OP_NOT_EXISTS: _build_query_where_exists,
        WHERE_OP_IS_NULL: _build_query_where_is_null,
        WHERE_OP_IS_NOT_NULL: _build_query_where_is_null,
    }
    assert sorted(_build_query_where_map.keys()) == sorted(WHERE_OPS)
