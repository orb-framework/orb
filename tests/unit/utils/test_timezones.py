def test_localize_timezone_with_no_timezones():
    import datetime
    from orb.utils.timezones import localize_timezone

    d = datetime.datetime(2000, 1, 1)
    d2 = localize_timezone(d)

    assert d == d2


def test_localize_timezone_switch_zones():
    import datetime
    import pytz

    from orb.utils.timezones import localize_timezone

    pacific_zone = pytz.timezone('US/Pacific')

    d = datetime.datetime(2000, # year
                          1, # month
                          1, # day
                          12, # hour
                          30, # minute
                          30) # second

    pst_dtime = pacific_zone.localize(d)

    d2 = localize_timezone(d)
    d3 = localize_timezone(pst_dtime, tz=pacific_zone)
    d4 = localize_timezone(pst_dtime, tz=pytz.utc)

    f = '%Y-%m-%d %H:%M:%S'

    assert d.strftime(f) == '2000-01-01 12:30:30'
    assert d2.strftime(f) == '2000-01-01 12:30:30'
    assert d3.strftime(f) == '2000-01-01 12:30:30'
    assert d4.strftime(f) == '2000-01-01 20:30:30'


def test_utc_timezone_conversion():
    import datetime
    import pytz

    from orb.utils.timezones import utc_timezone

    pacific_zone = pytz.timezone('US/Pacific')

    d = datetime.datetime(2000, # year
                          1, # month
                          1, # day
                          12, # hour
                          30, # minute
                          30) # second

    pst_d = pacific_zone.localize(d)

    d2 = utc_timezone(d)
    d3 = utc_timezone(pst_d)

    f = '%Y-%m-%d %H:%M:%S'

    assert d2.strftime(f) == '2000-01-01 20:30:30'
    assert d3.strftime(f) == '2000-01-01 20:30:30'

