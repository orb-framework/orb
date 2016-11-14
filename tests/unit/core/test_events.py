def test_single_shot_callback():
    import blinker
    from orb.core import events

    checked = {}
    def test_callback_method(value, option=False):
        checked['callback'] = (value, option)
        return True

    # create the callback instance
    signal = blinker.Signal()
    callback = events.Callback(test_callback_method,
                               signal=signal,
                               args=(10,),
                               kwargs={'option': True})

    signal.send(event=None)

    assert checked['callback'] == (10, True)
    checked.clear()

    signal.send(event=None)
    assert checked == {}


def test_recurring_callback():
    import blinker
    from orb.core import events

    checked = {}
    def test_callback_method(value, option=False):
        checked['callback'] = (value, option)
        return True

    # create the callback instance
    signal = blinker.Signal()
    callback = events.Callback(test_callback_method,
                               signal=signal,
                               args=(10,),
                               kwargs={'option': True},
                               single_shot=False)

    signal.send(event=None)

    assert checked['callback'] == (10, True)
    checked.clear()

    signal.send(event=None)
    assert checked['callback'] == (10, True)


def test_basic_event():
    from orb.core import events

    event = events.Event()
    assert event.prevent_default is False


def test_database_event():
    from orb.core import events

    event = events.DatabaseEvent(database='orb')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.DatabaseEvent)

    assert event.prevent_default is False
    assert event.database == 'orb'


def test_connection_event():
    from orb.core import events

    event = events.ConnectionEvent(success=False,
                                   native='status',
                                   database='orb')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.DatabaseEvent)
    assert isinstance(event, events.ConnectionEvent)

    assert event.prevent_default is False
    assert event.database == 'orb'
    assert event.success is False
    assert event.native == 'status'


def test_model_event():
    from orb.core import events

    event = events.ModelEvent(model='User')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.ModelEvent)

    assert event.prevent_default is False
    assert event.model == 'User'


def test_sync_event():
    from orb.core import events

    event = events.SyncEvent(context={'returning': 'schema'}, model='User')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.ModelEvent)
    assert isinstance(event, events.SyncEvent)

    assert event.prevent_default is False
    assert event.model == 'User'
    assert event.context == {'returning': 'schema'}


def test_record_event():
    from orb.core import events

    event = events.RecordEvent(record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)

    assert event.prevent_default is False
    assert event.record == {'id': 10}


def test_change_event():
    from orb.core import events

    event = events.ChangeEvent(changes={'username': ('jdoe', 'john.doe')}, record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.ChangeEvent)

    assert event.prevent_default is False
    assert event.record == {'id': 10}
    assert event.changes.keys() == ['username']
    assert event.changes['username'] == ('jdoe', 'john.doe')


def test_change_event_inflation():
    import orb
    from orb.core import events

    class CustomModel(object):
        def __init__(self, id):
            self.id = id

    class CustomColumn(orb.ReferenceColumn):
        def reference_model(self):
            return CustomModel

    col = CustomColumn()
    event = events.ChangeEvent(changes={col: (1, 2)})

    old_value, new_value = event.inflated_changes[col]

    assert isinstance(new_value, CustomModel)
    assert new_value.id == 2

    assert isinstance(old_value, CustomModel)
    assert old_value.id == 1


def test_delete_event():
    from orb.core import events

    event = events.DeleteEvent(record={'id': 1},
                               context={'returning': 'schema'})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.DeleteEvent)

    assert event.prevent_default is False
    assert event.record == {'id': 1}
    assert event.context == {'returning': 'schema'}


def test_delete_event_processing():
    from orb.core import events

    class MockModel(object):
        def __init__(self, prevent):
            self.prevent = prevent

        def on_delete(self, event):
            event.prevent_default = self.prevent

    event = events.DeleteEvent()

    a = MockModel(prevent=False)
    b = MockModel(prevent=True)

    results = event.process([a, b])
    assert list(results) == [a]


def test_init_event():
    from orb.core import events

    event = events.InitEvent(record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.InitEvent)

    assert event.prevent_default is False
    assert event.record == {'id': 10}


def test_save_event():
    from orb.core import events

    event = events.SaveEvent(context={'expand': 'user'},
                             new_record=True,
                             changes={'username': ('jdoe', 'john.doe')},
                             result=False,
                             record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.SaveEvent)

    assert event.prevent_default is False
    assert event.record == {'id': 10}
    assert event.context == {'expand': 'user'}
    assert event.new_record is True
    assert event.changes == {'username': ('jdoe', 'john.doe')}
    assert event.result is False

