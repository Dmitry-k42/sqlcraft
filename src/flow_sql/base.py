import json
import re
from collections.abc import Mapping, Iterable

from psycopg2 import sql

from .misc import alias, expr


class BaseCommand:
    def __init__(self, conn):
        self._conn = conn
        self._params = {}
        self._user_params = {}
        self._param_name_prefix = None
        self._subqueries_built = 0
        self._params_next_idx = 0
        self.json_dump_fn = json.dumps

    def execute(self):
        """
        Execute the query. Returns a new cursor which can be used to iterate the query result
        Usage example:
        cursor = qb.execute()
        for row in cursor:
            ... do what you want
        :return: cursor
        """
        cursor = self._conn.cursor()
        query = self._build_query()
        cursor.execute(query, self._params)
        return cursor

    def all(self, eager=False, as_dict=False):
        """
        Returns a generator (or list) for enumerating query result rows
        :param eager - if True returns list of resulting rows, else generator will be returned
        :param as_dict - set it True if you need to fetch rows as dict objects. If it's False then rows returns as DBRow
        :return:
        """
        if eager:
            return list(self.all(eager=False, as_dict=as_dict))
        else:
            def inner_gen():
                cursor = self.execute()
                for row in cursor:
                    yield dict(row.items()) if as_dict else row
            return inner_gen()

    def one(self, as_dict=False):
        """
        Returns a single row of the query result
        :param as_dict - set it True if you need to fetch a dict object. If it's False then you will get a DBRow object
        :return:
        """
        cursor = self.execute()
        row = cursor.fetchone()
        return dict(row.items()) if as_dict and row is not None else row

    def scalar(self):
        """
        Returns a single scalar value returned after query execution
        :return:
        """
        cursor = self.execute()
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def column(self, eager=False):
        """
        Returns a single column of the query result
        :param eager - if True returns a list of values else a generator will be returned
        :return:
        """
        if eager:
            return list(self.column(eager=False))
        else:
            def inner_gen():
                cursor = self.execute()
                for row in cursor:
                    col = row[0]
                    yield col
            return inner_gen()

    def as_string(self):
        """
        Returns the query as raw sql statement
        :return: str
        """
        return self._build_query().as_string(self._conn)

    def _build_query(self, param_name_prefix=None):
        self._params = {k: v for k, v in self._user_params.items()}
        self._params_next_idx = 0
        self._subqueries_built = 0
        self._param_name_prefix = param_name_prefix if param_name_prefix is not None else 'p'
        return sql.SQL('')

    def _set_param(self, value, json_stringify=False):
        while True:
            param_name = '{}{}'.format(self._param_name_prefix, self._params_next_idx)
            if param_name not in self._params:
                break
            self._params_next_idx += 1
        self._params[param_name] = self._prepare_param_value(value, json_stringify)
        return sql.Placeholder(param_name)

    def get_next_param_name(self, prefix='u'):
        i = 0
        while True:
            param_name = '{}{}'.format(prefix, i)
            if param_name not in self._user_params:
                return param_name
            i += 1

    def add_param(self, param_name, value, json_stringify=False):
        self._user_params[param_name] = self._prepare_param_value(value, json_stringify)
        return self

    def add_params(self, params, json_stringify=False):
        for key, value in params.items():
            self.add_param(key, value, json_stringify)
        return self

    def params(self, params, json_stringify=False):
        self._user_params = {}
        return self.add_params(params, json_stringify)

    def _prepare_param_value(self, value, json_stringify=False):
        if json_stringify and not isinstance(value, str) and isinstance(value, (Mapping, Iterable)):
            value = self.json_dump_fn(value)
        return value

    def _build_subquery(self, subquery):
        res = subquery._build_query(param_name_prefix='p%d_' % self._subqueries_built)
        for k, v in subquery._params.items():
            self._params[k] = v
        self._subqueries_built += 1
        return res

    def quoted(self, s):
        """
        Quotes identifiers in the given string
        :param s: string
        :return: string
        """
        return self._quote_string(s).as_string(self._conn)

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

    def _quote_string(self, val) -> sql.Composable:
        if isinstance(val, sql.Composable):
            return val
        if isinstance(val, BaseCommand):
            return sql.SQL('({})').format(self._build_subquery(val))
        if isinstance(val, alias):
            quoted_ident = self._quote_string(val.ident)
            return quoted_ident if val.alias is None \
                else sql.SQL('{} AS {}').format(quoted_ident, self._quote_string(val.alias))
        if isinstance(val, expr):
            return sql.SQL(val.value)
        if not isinstance(val, str):
            return sql.Literal(val)
        if re.search(r'[\(\)]', val):
            return sql.SQL(val)
        return sql.SQL('').join([
            self._quote_identifier(substr) if self._valid_identifier(substr) else sql.SQL(substr)
            for substr in self._split_identifiers(val)
        ])

    _quote_column = _quote_string
    _quote_table = _quote_string

    @staticmethod
    def _parse_column_or_table(val, _alias=None):
        if isinstance(val, alias):
            return val
        if _alias is None and isinstance(val, str):
            match = re.fullmatch(r'^([\w\.]+)\s+(as\s+)?(\w+)$', val, re.IGNORECASE)
            if match:
                return alias(ident=match.group(1), alias=match.group(3))
        return alias(val, _alias)

    def _place_value(self, value):
        if isinstance(value, sql.SQL):
            val = value
        elif isinstance(value, expr):
            val = self._quote_string(value.value)
        elif isinstance(value, BaseCommand):
            val = self._quote_string(value)
        else:
            val = self._set_param(value)
        return val
