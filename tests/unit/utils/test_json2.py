import datetime
import pytest


def test_dumps_custom_objects():
    from orb.utils import json2

    class MyObject(object):
        def __json__(self):
            return {'class': 'MyObject'}

    obj = MyObject()
    jdata = json2.dumps(obj, indent=None)
    assert jdata == '{"class": "MyObject"}'

    class MyObject(object):
        __json__ = {'class': 'MyObject'}

    obj = MyObject()
    jdata = json2.dumps(obj, indent=None)
    assert jdata == '{"class": "MyObject"}'


def test_dumps_datetime_object():
    import datetime
    from orb.utils import json2

    dtime = datetime.datetime(2016, 10, 8, 1, 0, 0)
    jdata = json2.dumps({'dtime': dtime}, indent=None)
    assert jdata == '{"dtime": "2016-10-08T01:00:00"}'


def test_dumps_date_object():
    import datetime
    from orb.utils import json2

    date = datetime.date(2016, 10, 8)
    jdata = json2.dumps({'date': date}, indent=None)
    assert jdata == '{"date": "2016-10-08"}'


def test_dumps_time_object():
    import time
    from orb.utils import json2

    time = datetime.time(1, 0, 0)
    jdata = json2.dumps({'time': time}, indent=None)
    assert jdata == '{"time": "01:00:00"}'


def test_dumps_set_object():
    import re
    from orb.utils import json2

    jdata = json2.dumps({'test', 'test2'}, indent=None)
    assert re.match('\["test(2)?", "test(2)?"\]', jdata)


def test_dumps_decimal():
    from orb.utils import json2
    import decimal

    jdata = json2.dumps(decimal.Decimal('2.2'), indent=None)
    assert jdata == '"2.2"'


def test_dumps_failure():
    from orb.utils import json2

    class MyObject(object):
        pass

    obj = MyObject()
    with pytest.raises(TypeError):
        assert not json2.dumps(obj)


def test_loads_custom_data():
    from orb.utils import json2

    jdata = """
    {
        "integer": 10,
        "text": "this is a test",
        "datetime": "2016-10-08T01:00:00",
        "date": "2016-10-08",
        "time": "01:00:00",
        "nested": {
            "integer": 10
        }
    }
    """

    pydata = json2.loads(jdata)
    assert pydata['integer'] == 10
    assert pydata['text'] == 'this is a test'
    assert pydata['datetime'] == datetime.datetime(2016, 10, 8, 1, 0, 0)
    assert pydata['date'] == datetime.date(2016, 10, 8)
    assert pydata['time'] == datetime.time(1, 0, 0)
    assert pydata['nested']['integer'] == 10

