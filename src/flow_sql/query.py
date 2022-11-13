import functools
from collections.abc import Sized, Mapping, Sequence, Iterable

from psycopg2 import sql

from .constants import ORDER_ASC, ORDER_DESC, JOIN_FULL, JOIN_LEFT, JOIN_RIGHT, JOIN_INNER
from .misc import alias, join, order, const
from .base import BaseCommand
from .behaviours import FromBehaviour, WhereBehaviour, WithBehaviour


class Query(BaseCommand, WhereBehaviour, WithBehaviour, FromBehaviour):
    """
    Builder for SELECT query
    """
    def __init__(self, conn):
        super().__init__(conn)
        WhereBehaviour.__init__(self, self)
        WithBehaviour.__init__(self, self)
        FromBehaviour.__init__(self, self)
        self._distinct = False
        self._select = []
        self._join = []
        self._group = []
        self._order = []
        self._limit = None

    def distinct(self, _distinct=True):
        self._distinct = _distinct
        return self

    def select(self, fields):
        """
        Sets SELECT query block. `fields` parameter maybe string or iterable object
        Usage examples:
        * Comma-separated fields:
            select('id, name, u.address')
                => SELECT "id", "name", "u"."address"
        * List of fields:
            select(['id', 'name', 'u.address'])
                => SELECT "id", "name", "u"."address"
        * Tuple:
            select(('id', 'name')) => SELECT "id", "name"
        * Set:
            select({'id', 'name'}) => SELECT "name", "id"
        * All fields:
            select('*') => SELECT *
        * Constant:
            select(1) => SELECT 1
            select(true) => SELECT true
            select(None) => SELECT NULL
        * Expressions: If the field contains "(" or ")" symbol then it will not be quoted
            select('count(*)') => SELECT count(*)
            select('avg(user.age)') => SELECT avg(user.age)
        * AS-syntax:
            select('c.name AS city_name') => SELECT "c"."name" AS "city_name"
            select('c.name city_name') => SELECT "c"."name" AS "city_name"
        * Subquery:
            subq = Query(conn).select('count(*)').from('cars c').where('c.user_id=u.id')
            select(subq)
                => SELECT (SELECT count(*) FROM "cars" "c" WHERE "c"."user_id"="u"."id")
            subq = Query(conn).select('count(*)').from('cars c').where('c.user_id=u.id')
            select(alias(subq, alias='cars_count'))
                => SELECT (SELECT count(*) FROM "cars" "c" WHERE "c"."user_id"="u"."id") AS "cars_count"
        :param fields: the fields to add
        :return: self
        """
        self._select = []
        self.add_select(fields)
        return self

    def add_select(self, fields):
        """
        Appends new fields to SELECT block. See `select` method for examples
        :param fields: the fields to add
        :return: self
        """
        if isinstance(fields, alias):
            self._select.append(fields)
            return self
        if isinstance(fields, str):
            fields = fields.split(',')
        elif not isinstance(fields, Iterable):
            fields = [fields]
        for field in fields:
            if isinstance(field, str):
                field = self._parse_column_or_table(field.strip())
            self._select.append(field)
        return self

    def _join_table(self, join_type, table, on=None, alias=None, lateral=False):
        """
        Adds a new JOIN query block. There are following methods available:
        * `join_full` - for JOIN FULL
        * `join_left` - for JOIN LEFT
        * `join_right` - for JOIN RIGHT
        * `join_` - for JOIN
        `table` & `alias` parameters works absolutely same as it was described in docs for `from_` method.
        Please check it there to learn more about it.
        Usage examples:
        * join_('auth.users u', 'e.user_id=u.id')
            => JOIN "auth"."users" AS "u" ON "e"."user_id"="u"."id"
        * join_left('geo.city', alias='c', on='u.home_city_id=c.id')
            => JOIN LEFT "geo"."city" AS "c" ON "u"."home_city_id"="c"."id"
        * Without `on` param:
            join_('auth.users') => JOIN "auth"."users"
        * Subqueries supported. See example below:
            subq = Query(conn).select('*').from('cars')
            join_(subq, alias=c, on='c.user_id=u.id')
                => JOIN (SELECT * FROM "cars") AS "c" ON "c"."user_id"="u"."id"
        :param join_type: available values JOIN_LEFT, JOIN_RIGHT, JOIN_INNER, JOIN_FULL
        :param table: joining table name
        :param on: joining table condition. Can be ommited
        :param alias: joining table alias. Can be ommited
        :param lateral: flag for LATERAL keyword
        :return: self
        """
        self._join.append(join(join_type, self._parse_column_or_table(table, alias), self._parse_where_cond(on), lateral))
        return self

    join_left = functools.partialmethod(_join_table, JOIN_LEFT)
    join_right = functools.partialmethod(_join_table, JOIN_RIGHT)
    join_ = functools.partialmethod(_join_table, JOIN_INNER)
    join_full = functools.partialmethod(_join_table, JOIN_FULL)

    def group(self, fields):
        """
        Sets GROUP BY query block
        Usage examples:
        * Coma-separated string:
            group('id, name') => GROUP BY "id", "name"
        * Iterable object:
            group(['id', 'name']) => GROUP BY "id", "name"
        :param fields: fields for GROUP BY query block
        :return: self
        """
        self._group = []
        self.add_group(fields)
        return self

    def add_group(self, fields):
        """
        Adds a new fields to the GROUP BY query block.
        See `group` method for examples
        :param fields: fields for GROUP BY query block
        :return: self
        """
        if isinstance(fields, str):
            fields = fields.split(',')
        self._group.extend([field.strip() for field in fields])
        return self

    def order(self, fields):
        """
        Sets ORDER BY query block
        Usage examples:
        * Coma-separated fields:
            order('u.name, u.age DESC')
                => ORDER BY "u"."name", "u"."age" DESC
        * Iterable object:
            order(['u.name', 'u.age DESC'])
                => ORDER BY "u"."name", "u"."age" DESC
            order(['id', ORDER_DESC])
                => ORDER BY "id" DESC
            order(['u.name', (u.age, ORDER_DESC)])
                => ORDER BY "u"."name", "u"."age" DESC
        * `order` object:
            order(order('a.email', ORDER_ASC))
                => ORDER BY "a"."email" ASC
        * dictionary:
            order({'field': 'a.email', 'sort': ORDER_ASC})
                => ORDER BY "a"."email" ASC
        :param fields: the fields for ORDER BY query block
        :return: self
        """
        self._order = []
        self.add_order(fields)
        return self

    def add_order(self, field):
        """
        Adds new fields to the ORDER BY query block. See `order` method for examples
        :param field: the new fields to add
        :return: self
        """
        if isinstance(field, str):
            f = field.split(',')
            if len(f) > 1:
                self.add_order(f)
            else:
                f = [s for s in field.split(' ') if len(s) > 0]
                if len(f) == 2 and f[1].upper() in [ORDER_ASC, ORDER_DESC]:
                    ident = f[0]
                    sort = f[1].upper()
                else:
                    ident = field
                    sort = None
                self._order.append(order(ident, sort))
        elif isinstance(field, order):
            self._order.append(field)
        elif isinstance(field, Mapping):
            self._order.append(order(field['field'], field.get('sort')))
        elif isinstance(field, Sized) and isinstance(field, Sequence) and len(field) == 2\
                and field[1] in (ORDER_ASC, ORDER_DESC):
            self._order.append(order(field[0], field[1]))
        elif isinstance(field, Iterable):
            for f in field:
                self.add_order(f)
        return self

    def limit(self, limit: int):
        """
        Sets LIMIT query block
        Usage example:
        * limit(15) => LIMIT 15
        :param limit: integer to limit result query rows
        :return: self
        """
        self._limit = limit
        return self

    def _build_query(self, param_name_prefix=None):
        super()._build_query(param_name_prefix)
        parts = [
            self._build_query_with(),
            self._build_query_select(),
            self._build_query_from(),
            self._build_query_join(),
            self._build_query_where(),
            self._build_query_group(),
            self._build_query_order(),
            self._build_query_limit(),
        ]
        res = sql.SQL(' ').join([p for p in parts if p is not None])
        return res

    def _build_query_select(self):
        prefix = sql.SQL('SELECT')
        if self._distinct:
            prefix = prefix + sql.SQL(' DISTINCT')
        if len(self._select) == 0:
            return prefix

        def build_field(field):
            if isinstance(field, const):
                return self._o._set_param(field.value)
            return self._quote_column(field)

        return prefix + sql.SQL(' ') + (sql.SQL(', ').join([
            build_field(field) for field in self._select
        ]))

    def _build_query_join(self):
        if len(self._join) == 0:
            return None
        res = []
        for j in self._join:
            join_type, table, on, lateral = j
            ji = sql.SQL('{join_type} JOIN{lateral} {table}').format(
                join_type=sql.SQL(join_type),
                lateral=sql.SQL(' LATERAL' if lateral else ''),
                table=self._quote_table(table),
            )
            if on:
                ji += sql.SQL(' ON ') + self._build_query_where_iter(on)
            res.append(ji)
        return sql.SQL(' ').join(res)

    def _build_query_group(self):
        if len(self._group) == 0:
            return None
        return sql.SQL('GROUP BY ') + (sql.SQL(', ').join([
            self._quote_column(field) for field in self._group
        ]))

    def _build_query_order(self):
        if len(self._order) == 0:
            return None
        return sql.SQL('ORDER BY ') + (sql.SQL(', ').join([
            sql.SQL('{}{}').format(self._quote_column(o.ident), sql.SQL(' ' + o.sort if o.sort else '')) for o in self._order
        ]))

    def _build_query_limit(self) -> sql.Composable:
        return sql.SQL('LIMIT {}').format(sql.Literal(self._limit)) if self._limit else None


Select = Query
