import os

import psycopg2
from dotenv import load_dotenv

from flow_sql.conn import Connection

load_dotenv()

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '5432'
DEFAULT_DATABASE = 'test'
DEFAULT_USER = 'test'
DEFAUL_PASSWORD = 'test'

CONN_HOST = os.environ.get('PYTEST_CONN_HOST', DEFAULT_HOST)
CONN_PORT = os.environ.get('PYTEST_CONN_PORT', DEFAULT_PORT)
CONN_DATABASE = os.environ.get('PYTEST_CONN_DATABASE', DEFAULT_DATABASE)
CONN_USER = os.environ.get('PYTEST_CONN_USER', DEFAULT_USER)
CONN_PASSWORD = os.environ.get('PYTEST_CONN_PASSWORD', DEFAUL_PASSWORD)


class TestConnection(Connection):
    def commit(self):
        # For testing purposes this connection should never commit changes
        self.rollback()


def open_test_connection(connection_factory=TestConnection):
    try:
        return psycopg2.connect(
            host=CONN_HOST, port=CONN_PORT, database=CONN_DATABASE, user=CONN_USER,
            password=CONN_PASSWORD, connection_factory=connection_factory)
    except psycopg2.OperationalError:
        raise Exception(
            """It seems the test database is inaccessible.
            Please check that you have created it.
            Default connection settings are:
              host="{}", port={}, database="{}", user="{}", password="{}".
            You can set your own settings in .env file in the root of the project.
            Example:
              PYTEST_CONN_HOST=<paste your address>
              PYTEST_CONN_PORT=<paste your port>
              PYTEST_CONN_DATABASE=<paste your database name>
              PYTEST_CONN_USER=<paste your user>
              PYTEST_CONN_PASSWORD=<paste your password>
            """.format(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE, DEFAULT_USER, DEFAUL_PASSWORD)
        )
