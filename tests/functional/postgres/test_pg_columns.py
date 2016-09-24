import pytest
import datetime
import decimal
import pytz


TEST_DATA = {'testing': 10}
TEST_DATE = datetime.date(2015, 10, 10)
TEST_DATETIME = datetime.datetime(2015, 10, 10, 10, 10, 10)
TEST_TIME = datetime.time(10, 10, 10)
TEST_DATETIME_TZ = pytz.timezone('US/Pacific').localize(datetime.datetime(2015, 10, 10, 10, 10, 10, 0))
TEST_INTERVAL = datetime.timedelta(days=5)

TEST_DECIMAL = decimal.Decimal('10.25')
TEST_FLOAT = 10.2
TEST_INTEGER = 10
TEST_LONG = 2**63-1

TEST_TEXT = 'simple string'
TEST_COLOR = '#ffffff'
TEST_DIRECTORY = '/var/tmp'
TEST_FILEPATH = '/var/tmp/test.txt'
TEST_EMAIL = 'bob@test.com'
TEST_HTML = '<b>something</b>'
TEST_URL = 'https://google.com'
TEST_XML = '<orb version="2016.0.0"></orb>'


def _test_save(record, column, value):
    record.set(column, value)
    record.save()

def test_pg_columns_save_boolean(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'bool', True)

def test_pg_columns_save_binary(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'binary', TEST_DATA)

def test_pg_columns_save_json(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'json', TEST_DATA)

def test_pg_columns_save_yaml(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'yaml', TEST_DATA)

def test_pg_api_save_query(orb, pg_db, pg_all_column_record):
    q = orb.Query('name') == 'bob'
    pg_all_column_record.set('query', q)
    pg_all_column_record.save()
    assert pg_all_column_record.get('query').__json__() == q.__json__()

def test_pg_columns_save_date(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'date', TEST_DATE)

def test_pg_columns_save_datetime(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'datetime', TEST_DATETIME)

def test_pg_columns_save_datetime_tz(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'datetime_tz', TEST_DATETIME_TZ)

def test_pg_columns_save_interval(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'interval', TEST_INTERVAL)

def test_pg_columns_save_time(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'time', TEST_TIME)

def test_pg_columns_save_timestamp(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'timestamp', TEST_DATETIME)

def test_pg_columns_save_utc_datetime(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'utc_datetime', TEST_DATETIME)

def test_pg_columns_save_utc_timestamp(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'utc_timestamp', TEST_DATETIME)

def test_pg_columns_save_decimal(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'decimal', TEST_DECIMAL)

def test_pg_columns_save_float(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'float', TEST_FLOAT)

def test_pg_columns_save_integer(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'integer', TEST_INTEGER)

def test_pg_columns_save_long(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'long', TEST_LONG)

def test_pg_columns_save_enum(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'enum', pg_all_column_record.Test.Ok)

def test_pg_columns_save_string(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'string', TEST_TEXT)

def test_pg_columns_save_text(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'text', TEST_TEXT)

def test_pg_columns_save_color(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'color', TEST_COLOR)

def test_pg_columns_save_directory(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'directory', TEST_DIRECTORY)

def test_pg_columns_save_email(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'email', TEST_EMAIL)

def test_pg_columns_save_filepath(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'filepath', TEST_FILEPATH)

def test_pg_columns_save_html(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'html', TEST_HTML)

def test_pg_columns_save_url(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'url', TEST_URL)

def test_pg_columns_save_xml(orb, pg_db, pg_all_column_record):
    _test_save(pg_all_column_record, 'xml', TEST_XML)

# ---

def test_pg_columns_restore_boolean(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('bool') == True

def test_pg_columns_restore_binary(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('binary') == TEST_DATA

def test_pg_columns_restore_json(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('json') == TEST_DATA

def test_pg_columns_restore_yaml(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('yaml') == TEST_DATA

def test_pg_columns_restore_query(orb, pg_db, pg_last_column_record):
    q = orb.Query('name') == 'bob'
    assert pg_last_column_record.get('query').__json__() == q.__json__()

def test_pg_columns_restore_date(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('date') == TEST_DATE

def test_pg_columns_restore_datetime(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('datetime') == TEST_DATETIME

def test_pg_columns_restore_datetime_tz(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('datetime_tz') == TEST_DATETIME_TZ

def test_pg_columns_restore_interval(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('interval') == TEST_INTERVAL

def test_pg_columns_restore_time(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('time') == TEST_TIME

def test_pg_columns_restore_timestamp(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('timestamp') == TEST_DATETIME

def test_pg_columns_restore_utc_datetime(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('utc_datetime') == TEST_DATETIME

def test_pg_columns_restore_utc_timestamp(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('utc_timestamp') == TEST_DATETIME

def test_pg_columns_restore_decimal(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('decimal') == TEST_DECIMAL

def test_pg_columns_restore_float(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('float') == TEST_FLOAT

def test_pg_columns_restore_integer(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('integer') == TEST_INTEGER

def test_pg_columns_restore_long(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('long') == TEST_LONG

def test_pg_columns_restore_enum(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('enum') == pg_last_column_record.Test.Ok

def test_pg_columns_restore_string(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('string') == TEST_TEXT

def test_pg_columns_restore_text(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('text') == TEST_TEXT

def test_pg_columns_restore_color(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('color') == TEST_COLOR

def test_pg_columns_restore_directory(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('directory') == TEST_DIRECTORY

def test_pg_columns_restore_email(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('email') == TEST_EMAIL

def test_pg_columns_restore_filepath(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('filepath') == TEST_FILEPATH

def test_pg_columns_restore_html(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('html') == TEST_HTML

def test_pg_columns_restore_url(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('url') == TEST_URL

def test_pg_columns_restore_xml(orb, pg_db, pg_last_column_record):
    assert pg_last_column_record.get('xml') == TEST_XML

def test_pg_extract_values(orb, TestAllColumns, GroupUser, Group):
    import datetime
    users = GroupUser.all()
    all_records = TestAllColumns.all()

    assert isinstance(all_records.values('date')[0], datetime.date)
    assert isinstance(all_records.values('datetime')[0], datetime.datetime)
    assert isinstance(users.values('group')[0], Group)
    assert not isinstance(users.values('group_id')[0], Group)

    group, group_id = users.values('group', 'group_id')[0]
    assert isinstance(group, Group)
    assert not isinstance(group_id, Group)

