"""
Defines a mock backend database connection
"""

import orb

from collections import defaultdict


class MockConnection(orb.Connection):
    def __init__(self, database, responses=None):
        super(MockConnection, self).__init__(database)

        self.counter = defaultdict(lambda: 0)
        self.responses = responses

    def onSync(self, event):
        assert isinstance(event, orb.events.SyncEvent)
        return self.next_response('onSync')

    def addNamespace(self, namespace, context):
        """
        Mocks a response for creating a namespace for a connection.

        :param namespace: <str>
        :param context: <orb.Context>
        """
        # validate inputs
        assert isinstance(namespace, basestring)
        assert isinstance(context, orb.Context)

        # return desired response
        return self.next_response('addNamespace')

    def alterModel(self, model, context, add=None, remove=None, owner=''):
        """
        Mocks a response for creating a model for a connection.

        :param model: <orb.Model>
        :param context: <orb.Context>
        :param add: <list> || None
        :param remove: <list> || None
        :param owner: <str>
        """
        # validate inputs
        assert issubclass(model, orb.Model)
        assert isinstance(model, orb.Context)
        assert (add is None or type(add) is list)
        assert (remove is None or type(remove) is list)

        # return the desired response
        return self.next_response('alterModel')

    def close(self):
        """
        Mocks the database close method
        """
        return self.next_response('close')

    def commit(self):
        """
        Mocks the database commit method.
        """
        return self.next_response('commit')

    def count(self, model, context):
        """
        Mocks the count for a model

        :param model: <orb.Model>
        :param context: <orb.Context>
        """
        # validate inputs
        assert issubclass(model, orb.Model)
        assert isinstance(model, orb.Context)

        # return the desired response
        return self.next_response('count')

    def createModel(self, model, context, owner='', includeReferences=True):
        """
        Mock creates a new table in the database based cff the inputted
        table information.

        :param      schema   | <orb.TableSchema>
                    options  | <orb.Context>

        :return     <bool> success
        """
        # validate inputs
        assert issubclass(model, orb.Model)
        assert isinstance(model, orb.Context)
        assert isinstance(owner, basestring)
        assert type(includeReferences) == bool

        # return the desired response
        return self.next_response('createModel')

    def delete(self, records, context):
        """
        Mock removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.

        :param      table     | <orb.Collection>
                    context   | <orb.Context>

        :return     <int> | number of rows removed
        """
        # validate inputs
        assert isinstance(records, orb.Collection)
        assert isinstance(context, orb.Context)

        # return the desired response
        return self.next_response('delete')

    def execute(self, command, data=None, flags=0):
        """
        Mock executes the inputted command into the current
        connection cursor.

        :param      command  | <str>
                    data     | <dict> || None
                    flags    | <orb.DatabaseFlags>

        :return     <variant> returns a native set of information
        """
        # validate inputs
        assert isinstance(command, basestring)
        assert (data is None or type(data) == dict)
        assert type(flags) == int

        # return the desired response
        return self.next_response('execute')

    def insert(self, records, context):
        """
        Mock inserts the database record into the database with the
        given values.

        :param      records     | <orb.Collection>
                    context     | <orb.Context>

        :return     <bool>
        """
        # validate inputs
        assert isinstance(records, orb.Collection)
        assert isinstance(context, orb.Context)

        # return the desired response
        return self.next_response('insert')

    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.

        :return     <bool> connected
        """
        return self.next_response('isConnected')

    def interrupt(self, threadId=None):
        """
        Interrupts/stops the database access through a particular thread.

        :param      threadId | <int> || None
        """
        assert (threadId is None or type(threadId) == int)
        return self.next_response('interrupt')

    def next_response(self, method):
        """
        Returns the next response for a particular method call.  This
        can be used to drive the connection response information.

        :param method: <str>

        :return: <variant>
        """
        self.counter[method] += 1

        resp = self.responses.get(method)

        # responses can be provided a list for ordered response execution
        if isinstance(resp, list):
            try:
                return resp.pop()
            except IndexError:
                return None

        # or as a default value for all responses to a method
        else:
            return resp

    def open(self, force=False):
        """
        Opens a new database connection to the database defined
        by the inputted database.  If the force parameter is provided, then
        it will re-open a connection regardless if one is open already

        :param      force | <bool>

        :return     <bool> success
        """
        return self.next_response('open')

    def rollback(self):
        """
        Rolls back the latest code run on the database.
        """
        return self.next_response('rollback')

    def schemaInfo(self, context):
        """
        Returns the schema information from the database.

        :return     <dict>
        """
        assert isinstance(context, orb.Context)

        return self.next_response('schemaInfo')

    def select(self, model, context):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.

        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.Context>

        :return     [<variant> result, ..]
        """
        assert issubclass(model, orb.Model)
        assert isinstance(context, orb.Context)

        return self.next_response('select')

    def setup(self, context):
        """
        Initializes the database with any additional information that is required.
        """
        assert isinstance(context, orb.Context)
        return self.next_response('setup')

    def update(self, records, context):
        """
        Updates the database record into the database with the
        given values.

        :param      record  | <orb.Table>
                    options | <orb.Context>

        :return     <bool>
        """
        assert isinstance(records, orb.Context)
        assert isinstance(context, orb.Context)

        return self.next_response('update')
