""" Defines the Transaction class to handle multiple database transactions """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software, LLC'
__license__         = 'LGPL'

__maintainer__      = 'Projex Software, LLC'
__email__           = 'team@projexsoftware.com'

class Transaction(object):
    _stack = []
    
    def __init__( self ):
        self._connections  = set()
        self._errors       = []
    
    def __enter__( self ):
        self.begin()
    
    def __exit__( self ):
        self.end()
    
    def commit( self ):
        """
        Commits the changes for this transaction.
        """
        for connection in self._connections:
            try:
                connection.commit()
            
            except Exception, err:
                connection.rollback()
    
    def begin( self ):
        """
        Begins a new transaction for this instance.
        """
        self._connections.clear()
        self._errors = []
        
        Transaction.push(self)
    
    def isErrored( self ):
        """
        Returns whether or not this transaction has an errored state.
        """
        return len(self._errors) > 0
    
    def errors( self ):
        """
        Returns the errors that occurred during this transaction.
        
        :return     [<subclass of Exception>, ..]
        """
        return self._errors
    
    def end( self ):
        """
        Commits all the changes for the database connections that have been
        processed while this transaction is active.
        """
        try:
            Transaction._stack.remove(self)
        except ValueError:
            pass
        
        if ( not self.isErrored() ):
            self.commit()
    
    def rollback( self, error ):
        """
        Rollsback the changes to the dirty connections for this instance.
        
        :param      error | <subclass of Exception>
        """
        self._errors.append(error)
        
        for connection in self._connections:
            connection.rollback()
    
    def setDirty( self, connection ):
        """
        Mark a given connection as being dirty.  The connection class will
        check the transaction information before committing any database
        code.  If there is an active transaction, then the connection
        will mark itself as dirty for this transaction.
        
        :param      connection | <orb.Connection>
        """
        self._connections.add(connection)
    
    @staticmethod
    def current():
        """
        Returns the current transaction for the system.
        
        :return     <Transaction> || None
        """
        if ( Transaction._stack ):
            return Transaction._stack[-1]
        return None
    
    @staticmethod
    def push( transaction ):
        """
        Pushes a new transaction onto the stack.
        
        :param     transaction | <Transaction>
        """
        Transaction._stack.append(transaction)
    
    @staticmethod
    def pop():
        """
        Removes the latest transaction from the stack.
        
        :return     <Transaction> || None
        """
        try:
            return Transaction._stack.pop()
        except IndexError:
            return None