"""SQL function shortcuts."""

from psycopg2 import sql


def now():
    """Shortcut for `now` SQL function. Return a query part to call it."""
    return sql.SQL('now()')
