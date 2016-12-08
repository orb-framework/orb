import pytest


def test_abstract_datetime_formatting():
    import orb
    import datetime

    dcol = orb.AbstractDatetimeColumn()
    assert dcol.default_format() == '%Y-%m-%d %H:%M:%S'
    assert dcol.restore('2000-01-01 00:00:00') == datetime.datetime(2000, 1, 1, 0, 0)
    assert dcol.restore(None) is None

    dcol.set_default_format('%Y-%m-%d')
    assert dcol.default_format() == '%Y-%m-%d'

    dcol = orb.AbstractDatetimeColumn(default_format='%Y-%m-%d')
    assert dcol.default_format() == '%Y-%m-%d'

    assert dcol.restore(None) is None
    assert dcol.restore('2000-01-01').date() == datetime.date(2000, 1, 1)

    assert dcol.value_from_string('2000-01-01') == datetime.datetime(2000, 1, 1)
    assert dcol.value_from_string(None) is None

    with pytest.raises(orb.errors.ColumnValidationError):
        assert dcol.value_from_string('failure') is None


def test_date_column_from_string():
    import orb
    import datetime

    d = orb.DateColumn()
    assert d.default_format() == '%Y-%m-%d'
    assert d.restore('2000-01-01') == datetime.date(2000, 1, 1)

def test_date_column_keywords():
    import orb
    import datetime

    d = orb.DateColumn()

    assert d.restore('today') == datetime.date.today()
    assert d.restore('now') == datetime.date.today()


def test_date_column_to_string():
    import orb
    import datetime

    d = orb.DateColumn()
    assert d.value_to_string(datetime.date(2000, 1, 1)) == '2000-01-01'


def test_datetime_column_to_string():
    import orb
    import datetime

    d = orb.DatetimeColumn()
    assert d.default_format() == '%Y-%m-%d %H:%M:%S'
    assert d.restore('2000-01-01 00:00:00') == datetime.datetime(2000, 1, 1)


def test_datetime_column_keywords():
    import orb
    import datetime

    d = orb.DatetimeColumn()

    assert abs((d.restore('today') - datetime.datetime.now()).total_seconds()) < 1
    assert abs((d.restore('now') - datetime.datetime.now()).total_seconds()) < 1


def test_datetime_with_timezone_restore():
    import orb
    import datetime
    import pytz

    server_tz = pytz.timezone(orb.system.settings.server_timezone)
    est_tz = pytz.timezone('US/Eastern')

    # create test scenarios
    test_string_without_tz = '2000-01-01 00:00:00'
    test_string_with_tz = '2000-01-01 00:00:00-0500'
    test_without_tz = datetime.datetime(2000, 1, 1)
    test_as_utc = pytz.utc.localize(datetime.datetime(2000, 1, 1))
    test_as_server_tz = server_tz.localize(datetime.datetime(2000, 1, 1))
    test_as_est_tz = est_tz.localize(datetime.datetime(2000, 1, 1))

    col = orb.DatetimeWithTimezoneColumn()

    # without timezone context
    a = col.restore(test_string_without_tz)
    b = col.restore(test_string_with_tz)
    c = col.restore(test_without_tz)
    d = col.restore(test_as_utc)
    e = col.restore(test_as_server_tz)
    f = col.restore(test_as_est_tz)

    # with UTC timezone context
    with orb.Context(timezone='UTC'):
        g = col.restore(test_string_without_tz)
        h = col.restore(test_string_with_tz)
        i = col.restore(test_without_tz)
        j = col.restore(test_as_utc)
        k = col.restore(test_as_server_tz)
        l = col.restore(test_as_est_tz)

    # with US/Eastern timezone context
    with orb.Context(timezone='US/Eastern'):
        m = col.restore(test_string_without_tz)
        n = col.restore(test_string_with_tz)
        o = col.restore(test_without_tz)
        p = col.restore(test_as_utc)
        q = col.restore(test_as_server_tz)
        r = col.restore(test_as_est_tz)

    t = '%Y-%m-%d %H:%M:%S %Z'

    assert a.strftime(t) == '2000-01-01 00:00:00 PST'  # assumes PST, returns PST
    assert b.strftime(t) == '1999-12-31 21:00:00 PST'  # given EST, returns PST
    assert c.strftime(t) == '2000-01-01 00:00:00 PST'  # assumes PST, returns PST
    assert d.strftime(t) == '1999-12-31 16:00:00 PST'  # given UTC, returns PST
    assert e.strftime(t) == '2000-01-01 00:00:00 PST'  # given PST, returns PST
    assert f.strftime(t) == '1999-12-31 21:00:00 PST'  # given EST, returns PST

    assert g.strftime(t) == '2000-01-01 08:00:00 UTC'  # assumes PST, returns UTC
    assert h.strftime(t) == '2000-01-01 05:00:00 UTC'  # given EST, returns UTC
    assert i.strftime(t) == '2000-01-01 08:00:00 UTC'  # assumes PST, returns UTC
    assert j.strftime(t) == '2000-01-01 00:00:00 UTC'  # given UTC, returns UTC
    assert k.strftime(t) == '2000-01-01 08:00:00 UTC'  # given PST, returns UTC
    assert l.strftime(t) == '2000-01-01 05:00:00 UTC'  # given EST, returns UTC

    assert m.strftime(t) == '2000-01-01 03:00:00 EST'  # assumes PST, returns EST
    assert n.strftime(t) == '2000-01-01 00:00:00 EST'  # given EST, returns EST
    assert o.strftime(t) == '2000-01-01 03:00:00 EST'  # assumes PST, returns EST
    assert p.strftime(t) == '1999-12-31 19:00:00 EST'  # given UTC, returns EST
    assert q.strftime(t) == '2000-01-01 03:00:00 EST'  # given PST, returns EST
    assert r.strftime(t) == '2000-01-01 00:00:00 EST'  # given EST, returns EST


def test_datetime_with_timezone_store():
    import orb
    import datetime
    import pytz

    col = orb.DatetimeWithTimezoneColumn()

    server_tz = pytz.timezone(orb.system.settings.server_timezone)
    est_tz = pytz.timezone('US/Eastern')

    a = col.store('2000-01-01')
    b = col.store('2000-01-01 00:00:00-0500')
    c = col.store(datetime.datetime(2000, 1, 1))
    d = col.store(pytz.utc.localize(datetime.datetime(2000, 1, 1)))
    e = col.store(server_tz.localize(datetime.datetime(2000, 1, 1)))
    f = col.store(est_tz.localize(datetime.datetime(2000, 1, 1)))

    with pytest.raises(orb.errors.ColumnValidationError):
        assert col.store('failure') is None

    t = '%Y-%m-%d %H:%M:%S %Z'

    assert a.strftime(t) == '2000-01-01 08:00:00 '  # assumes PST, stores UTC
    assert b.strftime(t) == '2000-01-01 05:00:00 '  # given EST, stores UTC
    assert c.strftime(t) == '2000-01-01 08:00:00 '  # assumes PST, stores UTC
    assert d.strftime(t) == '2000-01-01 00:00:00 '  # given UTC, stores UTC
    assert e.strftime(t) == '2000-01-01 08:00:00 '  # given PST, stores UTC
    assert f.strftime(t) == '2000-01-01 05:00:00 '  # given EST, stores UTC


def test_datetime_with_timezone_column_keywords():
    import orb
    import datetime
    import pytz
    from orb.utils.timezones import localize_timezone

    col = orb.DatetimeWithTimezoneColumn()

    server_tz = pytz.timezone(orb.system.settings.server_timezone)
    dnow = localize_timezone(datetime.datetime.now(), tz=server_tz)
    assert abs((col.restore('now') - dnow).seconds) <= 1
    assert abs((col.restore('today') - dnow).seconds) <= 1


def test_interval_column():
    import orb
    import datetime

    col = orb.IntervalColumn()
    assert col.value_from_string('4500').total_seconds() == 4.5
    assert col.value_from_string(None) is None

    with pytest.raises(orb.errors.ColumnValidationError):
        assert col.value_from_string('failure') is None

    assert col.value_to_string(datetime.timedelta(seconds=2)) == '2000'
    assert col.value_to_string(2) == '2'
    assert col.value_to_string(None) is None

    with pytest.raises(orb.errors.ColumnValidationError):
        assert col.value_to_string(False)


def test_time_column():
    import orb
    import datetime

    col = orb.TimeColumn()

    a = col.restore('4:30:10')
    b = col.restore(datetime.timedelta(seconds=60 * 60 * 4 + 60 * 30 + 10))
    c = col.restore(datetime.time(4, 30, 10))

    f = '%H:%M:%S'

    assert a.strftime(f) == '04:30:10'
    assert b.strftime(f) == '04:30:10'
    assert c.strftime(f) == '04:30:10'


def test_time_column_keywords():
    import orb
    import datetime

    col = orb.TimeColumn()

    rtime = col.restore('now')
    ntime = datetime.datetime.now().time()

    assert abs(rtime.hour - ntime.hour) == 0
    assert abs(rtime.minute - ntime.minute) == 0
    assert abs(rtime.second - ntime.second) == 0


def test_timestamp_column():
    import orb
    import datetime
    import time

    col = orb.TimestampColumn()

    date_time = datetime.datetime(2000, 1, 1, 4, 30, 10)
    unix_time = 946729810.0

    assert col.restore(unix_time) == date_time
    assert col.store(date_time) == unix_time
    assert col.restore('2000-01-01') == datetime.datetime(2000, 1, 1)
    assert col.store('2000-01-01') == datetime.datetime(2000, 1, 1)

    assert col.value_from_string(str(unix_time)) == date_time