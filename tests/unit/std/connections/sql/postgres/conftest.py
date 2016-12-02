import pytest

@pytest.fixture()
def pg_conn(mock_db):
    import orb
    return orb.PostgresConnection(mock_db())


@pytest.fixture()
def pg_templates():
    import orb
    return orb.PostgresConnection.get_templates()

