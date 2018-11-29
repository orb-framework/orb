import pytest


@pytest.mark.parametrize('email,is_valid', [
    ('test@test.com', True),
    ('test@test-test.com', True),
    ('test@test.test.com', True),
    ('test+test@test.com', False),
    ('test\'test@test.com', False),
])
def test_email_column(email, is_valid):
    from orb.core.column_types.string import EmailColumn
    from orb.errors import ColumnValidationError
    email_column = EmailColumn()
    if is_valid:
        assert email_column.validate(email) is is_valid
    else:
        with pytest.raises(ColumnValidationError):
            email_column.validate(email)