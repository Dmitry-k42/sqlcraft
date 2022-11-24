from sqlcraft import Command
from sqlcraft.base import BaseCommand
from tests.connection import open_test_connection


def test_inheritance():
    assert issubclass(Command, BaseCommand)


def test_command():
    sql_cmd = 'SHOW ALL'
    with open_test_connection() as conn:
        cmd = Command(conn, sql_=sql_cmd)
        assert cmd.as_string() == sql_cmd
        sql_cmd += ';'
        cmd.sql(sql_cmd)
        assert cmd.as_string() == sql_cmd
        res = cmd.all(eager=True)
        assert res
