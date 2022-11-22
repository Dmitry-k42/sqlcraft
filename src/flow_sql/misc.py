"""
The module declares useful types here:
 * `Alias` - a type for SQL aliasing
 * `Expr` - a wrapper to make builder to prevent quotting a value
 * `Const` - a type is used for build queries with placeholders
"""

from collections import namedtuple

Alias = namedtuple('Alias', ['ident', 'alias'])
Expr = namedtuple('Expr', ['value'])
Const = namedtuple('Const', ['value'])

WithSubquery = namedtuple('WithSubquery', ['subquery', 'recursive'])
Join = namedtuple('Join', ['join_type', 'alias', 'on', 'lateral'])

Order = namedtuple('Order', ['ident', 'sort'])

WhereCond = namedtuple('WhereCond', ['op', 'ident', 'value'])
WhereCondArr = namedtuple('WhereCondArr', ['op', 'conds'])
WhereCondRaw = namedtuple('WhereCondRaw', ['value'])
