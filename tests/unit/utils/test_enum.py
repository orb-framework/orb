import pytest

@pytest.fixture()
def mock_binary_enum():
    from orb.utils.enum import enum

    return enum('Required', 'Unique', 'Expanded')


def test_enum_definition(mock_binary_enum):
    assert mock_binary_enum.Required == 1
    assert mock_binary_enum.Unique == 2
    assert mock_binary_enum.Expanded == 4


def test_enum_get_by_key(mock_binary_enum):
    assert mock_binary_enum['Required'] == 1
    assert mock_binary_enum['Unique'] == 2

    with pytest.raises(KeyError):
        assert mock_binary_enum['Test']


def test_enum_get_by_value(mock_binary_enum):
    assert mock_binary_enum[1] == 'Required'
    assert mock_binary_enum[2] == 'Unique'
    assert mock_binary_enum[4] == 'Expanded'

    with pytest.raises(KeyError):
        assert mock_binary_enum[3]


def test_enum_get_by_call(mock_binary_enum):
    assert mock_binary_enum(1) == 'Required'
    assert mock_binary_enum('Required') == 1
    assert mock_binary_enum({'Required', 'Unique'}) == 3

    with pytest.raises(KeyError):
        assert mock_binary_enum('Test')


def test_enum_get_by_set(mock_binary_enum):
    values = mock_binary_enum.from_set({'Required', 'Unique'})
    assert values == 3

    with pytest.raises(KeyError):
        assert mock_binary_enum.from_set({'Required', 'Unique', 'Test'})


def test_enum_json(mock_binary_enum):
    assert mock_binary_enum.__json__() == {'Required': 1, 'Unique': 2, 'Expanded': 4}


def test_enum_value_as_set(mock_binary_enum):
    assert mock_binary_enum.to_set(3) == {'Required', 'Unique'}


def test_custom_enum():
    from orb.utils.enum import enum

    custom = enum(A=1, B=2, C=3)
    assert custom.A == 1
    assert custom.B == 2
    assert custom.C == 3


def test_enum_fetch_all_values(mock_binary_enum):
    all = mock_binary_enum.all()
    assert all == 7