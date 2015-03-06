""" Defines the Transaction class to handle multiple database transactions """

import threading

from collections import defaultdict
from projex.locks import ReadWriteLock, ReadLocker, WriteLocker


class Transaction(object):
    _stack = defaultdict(list)
    _stackLock = ReadWriteLock()
    
    def __init__(self, dryRun=False):
        self._dryRun = dryRun
        self._connections = set()
        self._errors = []
    
    def __enter__(self):
        self.begin()

    # noinspection PyUnusedLocal
    def __exit__(self, typ, error, traceback):
        if self._dryRun:
            self.cancel()
        else:
            if error:
                self._errors.append(error)
            self.end()

    def cancel(self):
        """
        Cancels all the current submissions.
        """
        for connection in self._connections:
            connection.rollback()

    def commit(self):
        """
        Commits the changes for this transaction.
        """
        for connection in self._connections:
            try:
                connection.commit()
            except StandardError:
                connection.rollback()

    def begin(self):
        """
        Begins a new transaction for this instance.
        """
        self._connections.clear()
        self._errors = []
        
        Transaction.push(self)
    
    def isErrored(self):
        """
        Returns whether or not this transaction has an errored state.
        """
        return len(self._errors) > 0
    
    def errors(self):
        """
        Returns the errors that occurred during this transaction.
        
        :return     [<subclass of Exception>, ..]
        """
        return self._errors
    
    def end(self, threadId=None):
        """
        Commits all the changes for the database connections that have been
        processed while this transaction is active.
        """
        # remove the transaction first, or the connection will not commit properly
        self.pop(self, threadId)

        if not self.isErrored():
            self.commit()
        else:
            for connection in self._connections:
                connection.rollback()
    
    def rollback(self, error):
        """
        Rolls back the changes to the dirty connections for this instance.
        
        :param      error | <subclass of Exception>
        """
        self._errors.append(error)
        
        for connection in self._connections:
            connection.rollback()
    
    def setDirty(self, connection):
        """
        Mark a given connection as being dirty.  The connection class will
        check the transaction information before committing any database
        code.  If there is an active transaction, then the connection
        will mark itself as dirty for this transaction.
        
        :param      connection | <orb.Connection>
        """
        self._connections.add(connection)
    
    @staticmethod
    def current(threadId=None):
        """
        Returns the current transaction for the system.
        
        :return     <Transaction> || None
        """
        threadId = threadId or threading.current_thread().ident
        with ReadLocker(Transaction._stackLock):
            stack = Transaction._stack.get(threadId)
            return stack[-1] if stack else None
    
    @staticmethod
    def push(transaction, threadId=None):
        """
        Pushes a new transaction onto the stack.
        
        :param     transaction | <Transaction>
        """
        threadId = threadId or threading.current_thread().ident
        with WriteLocker(Transaction._stackLock):
            Transaction._stack[threadId].append(transaction)
    
    @staticmethod
    def pop(transaction=None, threadId=None):
        """
        Removes the latest transaction from the stack.
        
        :return     <Transaction> || None
        """
        threadId = threadId or threading.current_thread().ident
        with WriteLocker(Transaction._stackLock):
            if transaction:
                try:
                    Transaction._stack[threadId].remove(transaction)
                except (KeyError, ValueError):
                    return None
            else:
                try:
                    Transaction._stack[threadId].pop()
                except IndexError:
                    return None


