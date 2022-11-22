"""
Defines `BaseCommand`, the base type for every SQL builder class.
"""

import json
import re
from collections.abc import Mapping, Iterable

from psycopg2 import sql

from .misc import Alias, Expr


def _prepare_param_value(value, json_stringify, json_dump_fn):
    if json_stringify and not isinstance(value, str) and isinstance(value, (Mapping, Iterable)):
        value = json_dump_fn(value)
    return value


class BuildContext:
    """
    Class for containing all the context during building queries.
    """

    def __init__(self, user_params, param_name_prefix, json_dump_fn):
        self.params = user_params.copy()
        self.param_name_prefix = param_name_prefix if param_name_prefix is not None else 'p'
        self.subqueries_built = 0
        self._params_next_idx = 0
        self.json_dump_fn = json_dump_fn

    def set_param(self, value, json_stringify=False):
        """Set a new (or update) query parameter."""
        while True:
            param_name = '{}{}'.format(self.param_name_prefix, self._params_next_idx)
            if param_name not in self.params:
                break
            self._params_next_idx += 1
        self.params[param_name] = _prepare_param_value(value, json_stringify, self.json_dump_fn)
        return sql.Placeholder(param_name)


class BuiltCommand:
    """
    Built command created after `BaseCommand.build_query` method invoke.
    """

    def __init__(self, conn, query, params):
        self.query = query
        self.params = params
        self.conn = conn

    def execute(self):
        """
        Execute the query. Return a new cursor which can be used to iterate the query result
        Usage example:
        cursor = qb.execute()
        for row in cursor:
            ... do what you want
        :return: cursor
        """
        cursor = self.conn.cursor()
        cursor.execute(self.query, self.params)
        return cursor

    def all(self, eager=False, as_dict=False):
        """
        Return a generator (or list) for enumerating query result rows
        :param eager - if True returns list of resulting rows, else generator will be returned
        :param as_dict - set it True if you need to fetch rows as dict objects.
            If it's False then rows returns as DBRow
        :return:
        """
        if eager:
            return list(self.all(eager=False, as_dict=as_dict))

        def inner_gen():
            cursor = self.execute()
            for row in cursor:
                yield dict(row.items()) if as_dict else row

        return inner_gen()

    def one(self, as_dict=False):
        """
        Return a single row of the query result
        :param as_dict - set it True if you need to fetch a dict object.
            If it's False then you will get a DBRow object
        :return:
        """
        cursor = self.execute()
        row = cursor.fetchone()
        return dict(row.items()) if as_dict and row is not None else row

    def scalar(self):
        """
        Return a single scalar value returned after query execution
        :return:
        """
        cursor = self.execute()
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def column(self, eager=False):
        """
        Return a single column of the query result
        :param eager - if True returns a list of values else a generator will be returned
        :return:
        """
        if eager:
            return list(self.column(eager=False))

        def inner_gen():
            cursor = self.execute()
            for row in cursor:
                col = row[0]
                yield col

        return inner_gen()

    def as_string(self):
        """
        Return the query as raw sql statement
        :return: str
        """
        return self.query.as_string(self.conn)


class BaseCommand:
    """
    The base class for all command builders.
    """

    def __init__(self, conn):
        """Create a new object."""
        self.conn = conn
        self._user_params = {}
        self.json_dump_fn = json.dumps

    def execute(self):
        """
        Execute the query. Return a new cursor which can be used to iterate the query result
        Usage example:
        cursor = qb.execute()
        for row in cursor:
            ... do what you want
        :return: cursor
        """
        return self.build_query().execute()

    def all(self, eager=False, as_dict=False):
        """
        Return a generator (or list) for enumerating query result rows
        :param eager - if True returns list of resulting rows, else generator will be returned
        :param as_dict - set it True if you need to fetch rows as dict objects.
            If it's False then rows returns as DBRow
        :return:
        """
        return self.build_query().all(eager=eager, as_dict=as_dict)

    def one(self, as_dict=False):
        """
        Return a single row of the query result
        :param as_dict - set it True if you need to fetch a dict object.
            If it's False then you will get a DBRow object
        :return:
        """
        return self.build_query().one(as_dict=as_dict)

    def scalar(self):
        """
        Return a single scalar value returned after query execution
        :return:
        """
        return self.build_query().scalar()

    def column(self, eager=False):
        """
        Return a single column of the query result
        :param eager - if True returns a list of values else a generator will be returned
        :return:
        """
        return self.build_query().column(eager=eager)

    def as_string(self):
        """
        Return the query as raw sql statement
        :return: str
        """
        return self.build_query().as_string()

    def build_query(self, param_name_prefix=None):
        """
        Build a query. Return an object to be used in `cursor.execute()` function.
        You can call the function manually. Often you don't need to call this method.
        Use methods `one`, `column`, `scalar` or `all`. These methods call `build_query`
        under the hood and return data in convenient form. Call `build_query` method if you
        know what you are doing.
        :return: BuiltCommand
        """
        ctx = BuildContext(self._user_params, param_name_prefix, self.json_dump_fn)
        return BuiltCommand(
            conn=self.conn,
            query = self._on_build_query(ctx),
            params=ctx.params,
        )

    # Argument `ctx` is unused here, but it is useful in derived classes.
    # Given that I want to preserve its name here. That's why I suppress
    # linter warning
    # pylint: disable=W0613
    def _on_build_query(self, ctx):
        return sql.SQL('')

    def add_param(self, param_name, value, json_stringify=False):
        """Append a new parameter to the query."""
        self._user_params[param_name] = _prepare_param_value(
            value, json_stringify, self.json_dump_fn)
        return self

    def add_params(self, params, json_stringify=False):
        """Append many parameters to the current query."""
        for key, value in params.items():
            self.add_param(key, value, json_stringify)
        return self

    def params(self, params, json_stringify=False):
        """Clear current query parameters and set a new ones instead of."""
        self._user_params = {}
        return self.add_params(params, json_stringify)

    def get_params(self):
        """
        Return the parameters to be passed to the query execution.
        :return: dict
        """
        return self._user_params.copy()

    def build_subquery(self, subquery, ctx):
        """Build a subquery regard to the current build context."""
        res = subquery.build_query(param_name_prefix='p{}_'.format(ctx.subqueries_built))
        for k, v in subquery.get_params().items():
            ctx.params[k] = v
        ctx.subqueries_built += 1
        return res

    def _quote_identifier(self, name: str) -> sql.Composable:
        if re.fullmatch(r'\d*', name):
            return sql.Literal(int(name))
        return sql.Identifier(*name.split('.'))

    @classmethod
    def _split_identifiers(cls, text: str):
        return re.split(r'\b', text)

    @classmethod
    def _valid_identifier(cls, text: str) -> bool:
        return len(text) > 0 and re.fullmatch(r'\w*', text)

    def quote_string(self, val, ctx) -> sql.Composable:
        """
        Convert an identifier (table of field name) to `sql.Composable` type.
        Do quoting if it is necessary. If `val` is a subquery, it will be rendered
        with `build_subquery` method.
        """
        if isinstance(val, sql.Composable):
            return val
        if isinstance(val, BaseCommand):
            built_cmd = self.build_subquery(val, ctx)
            return sql.SQL('({})'.format(built_cmd.as_string()))
        if isinstance(val, Alias):
            quoted_ident = self.quote_string(val.ident, ctx)
            return quoted_ident if val.alias is None \
                else sql.SQL('{} AS {}').format(quoted_ident, self.quote_string(val.alias, ctx))
        if isinstance(val, Expr):
            return sql.SQL(val.value)
        if not isinstance(val, str):
            return sql.Literal(val)
        if re.search(r'[\(\)]', val):
            return sql.SQL(val)
        return sql.SQL('').join([
            self._quote_identifier(substr) if self._valid_identifier(substr) else sql.SQL(substr)
            for substr in self._split_identifiers(val)
        ])

    @classmethod
    def parse_column_or_table(cls, val, _alias=None):
        """
        Parse a string to detect a column or table name and its alias. Return an `alias` object.
        Examples:
            1) Invoke with val = 'field_name' returns alias(ident='field_name', alias=None)
            1) Invoke with val = 'field_name AS age' returns alias(ident='field_name', alias='age')
        :return: a new `alias` instance
        """
        if isinstance(val, Alias):
            return val
        if _alias is None and isinstance(val, str):
            match = re.fullmatch(r'^([\w\.]+)\s+(as\s+)?(\w+)$', val, re.IGNORECASE)
            if match:
                return Alias(ident=match.group(1), alias=match.group(3))
        return Alias(val, _alias)

    def _place_value(self, value, ctx):
        if isinstance(value, sql.SQL):
            val = value
        elif isinstance(value, Expr):
            val = self.quote_string(value.value, ctx)
        elif isinstance(value, BaseCommand):
            val = self.quote_string(value, ctx)
        else:
            val = ctx.set_param(value)
        return val
