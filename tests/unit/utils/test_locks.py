def test_lock_switch():
    from orb.utils.locks import MutexSwitch
    import threading

    lock = threading.Lock()
    switch = MutexSwitch(lock)

    assert switch.counter == 0
    assert switch.lock == lock
    assert switch.lock.locked() is False

    switch.acquire()

    assert switch.counter == 1
    assert switch.lock.locked() is True

    switch.acquire()

    assert switch.counter == 2
    assert switch.lock.locked() is True

    switch.release()

    assert switch.counter == 1
    assert switch.lock.locked() is True

    switch.release()

    assert switch.counter == 0
    assert switch.lock.locked() is False


def test_reading_from_rw_lock():
    from orb.utils.locks import ReadWriteLock, ReadLocker

    lock = ReadWriteLock()

    with ReadLocker(lock):
        assert lock.no_readers.locked() is False
        assert lock.no_writers.locked() is True
        assert lock.read_switch.counter == 1
        assert lock.write_switch.counter == 0

        with ReadLocker(lock):
            assert lock.read_switch.counter == 2

        assert lock.read_switch.counter == 1

    assert lock.read_switch.counter == 0
    assert lock.no_readers.locked() is False
    assert lock.no_writers.locked() is False


def test_writing_to_rw_lock():
    from orb.utils.locks import ReadWriteLock, WriteLocker

    lock = ReadWriteLock()

    with WriteLocker(lock):
        assert lock.no_readers.locked() is True
        assert lock.no_writers.locked() is True
        assert lock.read_switch.counter == 0
        assert lock.write_switch.counter == 1

    assert lock.no_readers.locked() is False
    assert lock.no_writers.locked() is False
    assert lock.read_switch.counter == 0
    assert lock.write_switch.counter == 0