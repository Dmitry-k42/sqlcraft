"""
The module declares useful types here:
 * `alias` - a type for SQL aliasing
 * `expr` - a wrapper to make builder to prevent quotting a value
 * `const` - a type is used for build queries with placeholders
"""

from collections import namedtuple

alias = namedtuple('alias', ['ident', 'alias'])
expr = namedtuple('expr', ['value'])
const = namedtuple('constant', ['value'])

with_subquery = namedtuple('with_subquery', ['subquery', 'recursive'])
join = namedtuple('join', ['join_type', 'alias', 'on', 'lateral'])

order = namedtuple('order', ['ident', 'sort'])

where_cond = namedtuple('where_cond', ['op', 'ident', 'value'])
where_cond_arr = namedtuple('where_cond_arr', ['op', 'conds'])
where_cond_raw = namedtuple('where_cond_raw', ['value'])
