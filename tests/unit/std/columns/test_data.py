def test_basic_data_column():
    import orb

    checks = {}

    def loader(sdata):
        checks['loads'] = True
        return sdata

    def dumper(sdata):
        checks['dumps'] = True
        return sdata

    d = orb.DataColumn(loader=loader, dumper=dumper)
    assert d.store_value(None) is None
    assert d.store_value('testing') == 'testing'
    assert checks['dumps']

    assert d.restore_value(None) is None
    assert d.restore_value('testing') == 'testing'
    assert checks['loads']


def test_binary_column():
    import orb

    pickled_data = "S'testing'\np1\n."
    b = orb.BinaryColumn()
    assert b.store_value('testing') == pickled_data
    assert b.restore_value(pickled_data) == 'testing'


def test_json_column():
    import orb
    from orb.utils import json2

    jdata = {
        'id': 1,
        'name': 'testing'
    }
    sdata = json2.dumps(jdata)

    j = orb.JSONColumn()
    assert j.store_value(jdata) == sdata
    assert j.restore_value(sdata) == jdata


def test_yaml_column():
    import orb
    import textwrap

    ydata = {
        'id': 1,
        'name': 'testing',
        'subquery': {
            'testing': 10
        }
    }

    ytext = textwrap.dedent("""\
    id: 1
    name: testing
    subquery: {testing: 10}
    """)

    y = orb.YAMLColumn()
    assert y.store_value(ydata) == ytext
    assert y.restore_value(ytext) == ydata


def test_query_column():
    import orb
    import json
    import textwrap

    qdata = {
      "case_sensitive": False,
      "column": "test",
      "functions": [],
      "inverted": False,
      "math": [],
      "model": "",
      "op": "Is",
      "type": "query",
      "value": 1
    }

    q = orb.Query('test') == 1
    qcol = orb.QueryColumn()

    assert json.loads(qcol.store_value(q)) == qdata
    assert hash(qcol.restore_value(json.dumps(qdata))) == hash(q)
    assert qcol.restore_value(None) is None
