def test_callbacks():
    from orb.core import events

    checked = {}
    def test_callback_method(value, option=False):
        checked['callback'] = (value, option)
        return True

    # create the callback instance
    callback = events.Callback(test_callback_method, 10, option=True)

    assert callback(None) is True
    assert checked['callback'] == (10, True)


def test_basic_event():
    from orb.core import events

    event = events.Event()
    assert event.preventDefault is False


def test_database_event():
    from orb.core import events

    event = events.DatabaseEvent(database='orb')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.DatabaseEvent)

    assert event.preventDefault is False
    assert event.database == 'orb'


def test_connection_event():
    from orb.core import events

    event = events.ConnectionEvent(success=False,
                                   native='status',
                                   database='orb')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.DatabaseEvent)
    assert isinstance(event, events.ConnectionEvent)

    assert event.preventDefault is False
    assert event.database == 'orb'
    assert event.success is False
    assert event.native == 'status'


def test_model_event():
    from orb.core import events

    event = events.ModelEvent(model='User')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.ModelEvent)

    assert event.preventDefault is False
    assert event.model == 'User'


def test_sync_event():
    from orb.core import events

    event = events.SyncEvent(context={'returning': 'schema'}, model='User')

    assert isinstance(event, events.Event)
    assert isinstance(event, events.ModelEvent)
    assert isinstance(event, events.SyncEvent)

    assert event.preventDefault is False
    assert event.model == 'User'
    assert event.context == {'returning': 'schema'}


def test_record_event():
    from orb.core import events

    event = events.RecordEvent(record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)

    assert event.preventDefault is False
    assert event.record == {'id': 10}


def test_change_event():
    from orb.core import events

    event = events.ChangeEvent(column='username',
                               old='jdoe',
                               value='john.doe',
                               record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.ChangeEvent)

    assert event.preventDefault is False
    assert event.record == {'id': 10}
    assert event.column == 'username'
    assert event.old == 'jdoe'
    assert event.value == 'john.doe'


def test_change_event_inflation():
    import orb
    from orb.core import events

    class CustomModel(object):
        def __init__(self, id):
            self.id = id

    class CustomColumn(orb.ReferenceColumn):
        def reference_model(self):
            return CustomModel

    event = events.ChangeEvent(column=CustomColumn(),
                               old=1,
                               value=2)

    new_value = event.inflated_value
    old_value = event.inflated_old_value

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

    assert event.preventDefault is False
    assert event.record == {'id': 1}
    assert event.context == {'returning': 'schema'}


def test_delete_event_processing():
    from orb.core import events

    class MockModel(object):
        def __init__(self, prevent):
            self.prevent = prevent

        def onDelete(self, event):
            event.preventDefault = self.prevent

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

    assert event.preventDefault is False
    assert event.record == {'id': 10}


def test_load_event():
    from orb.core import events

    event = events.LoadEvent(data={'username': 'jdoe'}, record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.LoadEvent)

    assert event.preventDefault is False
    assert event.record == {'id': 10}
    assert event.data == {'username': 'jdoe'}


def test_save_event():
    from orb.core import events

    event = events.SaveEvent(context={'expand': 'user'},
                             newRecord=True,
                             changes={'username': ('jdoe', 'john.doe')},
                             result=False,
                             record={'id': 10})

    assert isinstance(event, events.Event)
    assert isinstance(event, events.RecordEvent)
    assert isinstance(event, events.SaveEvent)

    assert event.preventDefault is False
    assert event.record == {'id': 10}
    assert event.context == {'expand': 'user'}
    assert event.newRecord is True
    assert event.changes == {'username': ('jdoe', 'john.doe')}
    assert event.result is False


def test_pre_and_post_save_events():
    from orb.core import events

    pre_save_event = events.PreSaveEvent()
    post_save_event = events.PostSaveEvent()

    for event in (pre_save_event, post_save_event):
        assert isinstance(event, events.Event)
        assert isinstance(event, events.RecordEvent)
        assert isinstance(event, events.SaveEvent)

        assert event.preventDefault is False
