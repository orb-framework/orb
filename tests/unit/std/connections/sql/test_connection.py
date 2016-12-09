import pytest


def test_sql_connection_is_abstract():
    from orb.std.connections.sql.connection import SQLConnection

    with pytest.raises(Exception):
        assert SQLConnection() is None


# def test_sql_connection_alter_model(mock_sql_conn):
#     import orb
#
#     templ = """\
#     here
#     """
#
#     conn = mock_sql_conn(templates={
#         'alter_table.sql.jinja': templ
#     })
#
#     class User(orb.Table):
#         __register__ = False
#
#         id = orb.IdColumn()
#         username = orb.StringColumn()
#
#     assert conn.alter_model(User, orb.Context(), add={}, remove={}, owner='') is True

