from collections import namedtuple

from .constants import ORDER_ASC

alias = namedtuple('alias', ['ident', 'alias'])
expr = namedtuple('expr', ['value'])
const = namedtuple('constant', ['value'])

with_subquery = namedtuple('with_subquery', ['subquery', 'recursive'])
join = namedtuple('join', ['join_type', 'alias', 'on', 'lateral'])

order = namedtuple('order', ['ident', 'sort'])


def order_desc(ident, sort=ORDER_ASC):
    return order(ident=ident, sort=sort)


where_cond = namedtuple('where_cond', ['op', 'ident', 'value'])
where_cond_arr = namedtuple('where_cond_arr', ['op', 'conds'])
where_cond_raw = namedtuple('where_cond_raw', ['value'])
