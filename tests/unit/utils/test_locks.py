def test_lock_stack():
    from orb.utils.locks import LockStack
    import threading

    lock = threading.Lock()
    stack = LockStack(lock)

    assert stack.counter == 0
    assert stack.lock == lock
    assert stack.lock.locked() is False

    stack.acquire()

    assert stack.counter == 1
    assert stack.lock.locked() is True

    stack.acquire()

    assert stack.counter == 2
    assert stack.lock.locked() is True

    stack.release()

    assert stack.counter == 1
    assert stack.lock.locked() is True

    stack.release()

    assert stack.counter == 0
    assert stack.lock.locked() is False


def test_reading_from_rw_lock():
    from orb.utils.locks import ReadWriteLock

    lock = ReadWriteLock()

    with lock.reading():
        assert lock.no_readers.locked() is False
        assert lock.no_writers.locked() is True
        assert lock.read_stack.counter == 1
        assert lock.write_stack.counter == 0

        with lock.reading():
            assert lock.read_stack.counter == 2

        assert lock.read_stack.counter == 1

    assert lock.read_stack.counter == 0
    assert lock.no_readers.locked() is False
    assert lock.no_writers.locked() is False


def test_writing_to_rw_lock():
    from orb.utils.locks import ReadWriteLock

    lock = ReadWriteLock()

    with lock.writing():
        assert lock.no_readers.locked() is True
        assert lock.no_writers.locked() is True
        assert lock.read_stack.counter == 0
        assert lock.write_stack.counter == 1

    assert lock.no_readers.locked() is False
    assert lock.no_writers.locked() is False
    assert lock.read_stack.counter == 0
    assert lock.write_stack.counter == 0