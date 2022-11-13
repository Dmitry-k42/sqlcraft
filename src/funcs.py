from psycopg2 import sql


def now():
    return sql.SQL('now()')
