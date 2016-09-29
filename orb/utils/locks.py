"""
Defines a read/write lock system for threading locks.

The ReadWriteLock class will allow thread-safe access to
information, where you can have multiple reads occurring at
one time, but only one write.

Credit for this code goes to:
http://code.activestate.com/recipes/577803-reader-writer-lock-with-priority-for-writers
"""

import contextlib
import logging
import threading

log = logging.getLogger(__name__)


class LockStack(object):
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

        from orb.utils.locks import ReadWriteLock

        rw_lock = ReadWriteLock()
        data = {}

        def read_data(key):
            with rw_lock.reading():
                return data.get(key)

        def write_data(key, value):
            with rw_lock.writing():
                data[key] = value
    """
    def __init__(self):
        self.no_readers = threading.Lock()
        self.no_writers = threading.Lock()
        self.readers_queue = threading.Lock()
        self.read_stack = LockStack(self.no_writers)
        self.write_stack = LockStack(self.no_readers)

    def reader_acquire(self):
        """
        Aquires the read lock, assuming there are
        no writers active to allow for multiple reads.
        """
        self.readers_queue.acquire()
        self.no_readers.acquire()
        self.read_stack.acquire()
        self.no_readers.release()
        self.readers_queue.release()

    def reader_release(self):
        """
        Releases the write lock assuming there are no
        other readers accessing data at the same time.
        """
        self.read_stack.release()

    @contextlib.contextmanager
    def reading(self):
        """
        Provides a context for reading data protected by
        this lock.

        :usage

            lock = ReadWriteLock()
            data = {'name': 'sally'}

            with lock.reading():
                return data['name']
        """
        self.reader_acquire()
        yield self
        self.reader_release()

    def writer_acquire(self):
        """
        Acquires the writing lock by aquiring both the
        readers lock and the writers lock.
        """
        self.write_stack.acquire()
        self.no_writers.acquire()

    def writer_release(self):
        """
        Releases both the writer and readers locks.
        """
        self.no_writers.release()
        self.write_stack.release()

    @contextlib.contextmanager
    def writing(self):
        """
        Provides a context for writing data protected by
        this lock.

        :usage

            lock = ReadWriteLock()
            data = {'name': 'sally'}

            with lock.writing():
                data['name'] = 'bob'
        """
        self.writer_acquire()
        yield self
        self.writer_release()

