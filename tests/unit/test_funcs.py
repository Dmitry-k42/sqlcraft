from flow_sql import now
from psycopg2 import sql


def test_now():
    assert now() == sql.SQL('now()')
