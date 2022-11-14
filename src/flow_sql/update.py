"""
Implementation for UPDATE query.
"""

from psycopg2 import sql

from .base import BaseCommand
from .behaviours import WhereBehaviour, WithBehaviour, ReturningBehaviour, TableBehaviour


class Update(BaseCommand, WhereBehaviour, WithBehaviour, ReturningBehaviour, TableBehaviour):
    """
    Builder for UPDATE commands.
    Examples:
        1) q = Update(conn,
                table='users',
                fields={
                    'fullname': expr("concat(first_name, ' ', last_name)"),
                    'active': True,
                    'gender': 'male',
                },
                where=[
                    WHERE_OP_AND,
                    ['in', 'last_name', ['Doe', 'Smith']],
                    {'first_name': 'John'},
                ],
            )
            q.returning('id')
            for id in q.column():
                print(id)
            That code was constructed this query:
                UPDATE "users"
                SET "fullname"=concat(first_name, ' ', last_name),
                    "active"=true, "gender"='male'
                WHERE ("last_name" IN ('Doe', 'Smith')) AND ("first_name"='John')
                RETURNING "id"

        2) The 2nd approach in query constructing is with methods:
            q = Update(conn) \
                .table('users') \
                .set({
                    'fullname': expr("concat(first_name, ' ', last_name)"),
                    'active': True,
                }) \
                .add_set(field='gender', value='male') \
                .where(['in', 'last_name', ['Doe', 'Smith']]) \
                .and_where({'first_name': 'John'}) \
                .returning('id')
            This code constructs the same query as in the example above
    """
    def __init__(self, conn, table=None, alias=None, fields=None, where=None):
        super().__init__(conn)
        WhereBehaviour.__init__(self, self)
        WithBehaviour.__init__(self, self)
        ReturningBehaviour.__init__(self, self)
        TableBehaviour.__init__(self, self, table, alias)
        self._fields = {}
        if fields is not None:
            self.set(fields)
        if where is not None:
            self.where(where)

    def set(self, fields):
        """
        Sets the SET query block.
        Usage example:
            subquery = Query(conn).select(18)
            set({
                'id': 12,
                'name': expr("concat(users.first_name, ' ', users.last_name)"),
                'age': subquery,
            })
                => SET "id"=12,
                    "name"=concat(users.first_name, ' ', users.last_name),
                    "age"=(SELECT 1)
        :param fields: dictionary of the setting fields
        :return: self
        """
        self._fields = fields
        return self

    def add_set(self, field, value):
        """
        Adds a single item into the SET query block.
        Usage example:
            add_set('gender', 'male')
                => SET "gender"='male'
        :param field: field name
        :param value: value for the field
        :return: self
        """
        self._fields[field] = value
        return self

    def _build_query(self, param_name_prefix=None):
        super()._build_query(param_name_prefix)
        parts = [
            self._build_query_with(),
            self._build_query_update(),
            self._build_query_set(),
            self._build_query_where(),
            self._build_query_returning(),
        ]
        res = sql.SQL(' ').join([p for p in parts if p is not None])
        return res

    def _build_query_where(self):
        incorrect_where = False
        if self._where in [None]:
            incorrect_where = True
        if incorrect_where:
            raise Exception('Sorry empty WHERE block is restricted on UPDATE operations. '
                            'Please call where(True) if you have to update all rows in the table')
        return super()._build_query_where()

    def _build_query_update(self):
        if not self._table:
            return None
        res = sql.SQL('UPDATE ') + self._quote_table(self._table)
        return res

    def _build_query_set(self):
        res = []
        for field, value in self._fields.items():
            res.append(sql.SQL('{field}={value}').format(
                field=self._quote_string(field),
                value=self._place_value(value),
            ))
        if len(res) == 0:
            return None
        return sql.SQL('SET ') + sql.SQL(', ').join(res)
