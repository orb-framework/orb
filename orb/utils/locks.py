"""
Defines a read/write lock system for threading locks.

The ReadWriteLock class will allow thread-safe access to
information, where you can have multiple reads occurring at
one time, but only one write.

Credit for this code goes to:
http://code.activestate.com/recipes/577803-reader-writer-lock-with-priority-for-writers
"""

import logging
import threading

log = logging.getLogger(__name__)


class MutexSwitch(object):
    def __init__(self, lock):
        self.counter = 0
        self.lock = lock
        self.mutex = threading.Lock()

    def acquire(self):
        """
        Acquires the lock, assuming the stack
        request is the first call.
        """
        self.mutex.acquire()
        self.counter += 1
        if self.counter == 1:
            self.lock.acquire()
        self.mutex.release()

    def release(self):
        """
        Releases the lock, assuming the stack
        request is the last call.
        """
        self.mutex.acquire()
        self.counter -= 1
        if self.counter == 0:
            self.lock.release()
        self.mutex.release()


class ReadWriteLock(object):
    """
    Defines a read / write lock that will allow for multiple readers
    to access data at a single time, but only a signle writer access
    to modify data.

    :usage

        from orb.utils.locks import (
            ReadWriteLock,
            ReadLocker,
            WriteLocker
        )

        rw_lock = ReadWriteLock()
        data = {}

        def read_data(key):
            with ReadLocker(rw_lock):
                return data.get(key)

        def write_data(key, value):
            with WriteLocker(rw_lock):
                data[key] = value
    """
    def __init__(self):
        self.no_readers = threading.Lock()
        self.no_writers = threading.Lock()
        self.readers_queue = threading.Lock()
        self.read_switch = MutexSwitch(self.no_writers)
        self.write_switch = MutexSwitch(self.no_readers)

    def reader_acquire(self):
        """
        Aquires the read lock, assuming there are
        no writers active to allow for multiple reads.
        """
        self.readers_queue.acquire()
        self.no_readers.acquire()
        self.read_switch.acquire()
        self.no_readers.release()
        self.readers_queue.release()

    def reader_release(self):
        """
        Releases the write lock assuming there are no
        other readers accessing data at the same time.
        """
        self.read_switch.release()

    def writer_acquire(self):
        """
        Acquires the writing lock by aquiring both the
        readers lock and the writers lock.
        """
        self.write_switch.acquire()
        self.no_writers.acquire()

    def writer_release(self):
        """
        Releases both the writer and readers locks.
        """
        self.no_writers.release()
        self.write_switch.release()


class ReadLocker(object):
    """
    Context manager used for acquiring a read lock from a ReadWriteLock
    to access data.

    :usage

        from orb.utils.locks import ReadWriteLock, ReadLocker

        lock = ReadWriteLock()
        data = {'testing': 10}

        with ReadLocker(lock):
            a = data.get('testing')
    """
    def __init__(self, lock):
        self.lock = lock

    def __enter__(self):
        self.lock.reader_acquire()

    def __exit__(self, *args):
        self.lock.reader_release()


class WriteLocker(object):
    """
    Context manager used for acquiring a write lock from a ReadWriteLock
    to access data.

    :usage

        from orb.utils.locks import ReadWriteLock, WriteLocker

        lock = ReadWriteLock()
        data = {'testing': 10}

        with WriteLocker(lock):
            data['testing'] = 20
    """
    def __init__(self, lock):
        self.lock = lock

    def __enter__(self):
        self.lock.writer_acquire()

    def __exit__(self, *args):
        self.lock.writer_release()

