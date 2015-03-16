""" Defines the Join class used when querying multiple tables. """

import logging
from projex.lazymodule import lazy_import

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Join(object):
    """ 
    Defines a class for creating joined database lookups, returning
    multiple table records on a single select lookup.
    """

    def __init__(self, *table_or_queries, **options):
        """
        Joins together at least one Table class to be looked up from the
        database.  You can also provide a column query by passing in a
        query instance of a Table and columnName to only return the value
        from the given table instead of the inflated table object itself.
        
        :param      *args       ( <subclass of Table> || <Query>, .. )
        :param      **options   Additional keyword options
                    #. db       <orb.Database>
        
        :usage      |>>> from orb import Join as J, Query as Q
                    |>>> user = User.byFirstAndLastName('Eric','Hulser')
                    |>>> q  = Q(Role,'user')==user
                    |>>> q &= Q(Role,'primary') == True
                    |>>> q &= Q(Role,'department')==Q(Department)
                    |>>> J(Role,Department).selectFirst( where = q )
                    |(<Role>,<Department>)
                    |>>> q  = Q(Role,'user')==user
                    |>>> q &= Q(Role,'department')==Q(Department)
                    |>>> J(Q(Department,'name'),Q(Role,'primary')).select( where = True )
                    |[('Modeling',False),('Rigging',True)]
        """
        self._options = list(table_or_queries)
        self._database = options.get('db')

    def addOption(self, table_or_query):
        """
        Adds a new option to this join for lookup.
        
        :param      option      <subclass of orb.Table> || <orb.Query>
        """
        self._options.append(table_or_query)

    def database(self):
        """
        Returns the database instance that is linked to this join.
        
        :return     <orb.Database> || None
        """
        if self._database:
            return self._database

        # use the first database option based on the tables
        for option in self.options():
            if isinstance(option, orb.Table):
                return option.database()

        from orb import Orb

        return Orb.instance().database()

    def options(self):
        """
        Returns the options that this join is using.
        
        :return     [ <subclass of Table> || <Query>, .. ]
        """
        return self._options

    def selectFirst(self, **kwds):
        """
        Selects records for the class based on the inputted
        options.  If no db is specified, then the current
        global database will be used
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'options' for 
                    an instance of the <orb.ContextOptions>
        
        :return     (<variant>, ..)
        """
        db = kwds.get('db')
        lookup = orb.LookupOptions(**kwds)
        options = orb.ContextOptions(**kwds)

        if not db:
            db = self.database()

        if not db:
            raise errors.DatabaseNotFound()

        return db.backend().selectFirst(self, lookup, options)

    def select(self, **kwds):
        """
        Selects records for the joined information based on the inputted
        options.  If no db is specified, then the current
        global database will be used
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'options' for 
                    an instance of the <orb.ContextOptions>
        
        :return     [ ( <variant>, .. ), .. ]
        """
        db = kwds.get('db')
        lookup = orb.LookupOptions(**kwds)
        options = orb.ContextOptions(**kwds)

        if not db:
            db = self.database()

        if not db:
            raise errors.DatabaseNotFound()

        return db.backend().select(self, lookup, options)

    def setDatabase(self, database):
        """
        Sets the database instance that this join will use.
        
        :param      database   <orb.Database>
        """
        self._database = database

    def tables(self):
        """
        Returns the list of tables used in this join instance.
        
        :return     [<orb.Table>, ..]
        """
        output = []
        for option in self._options:
            if orb.Table.typecheck(option) or orb.View.typecheck(option):
                output.append(option)
            else:
                output += option.tables()
        return output

    @staticmethod
    def typecheck(obj):
        """
        Returns whether or not the inputted object is a type of a query.
        
        :param      obj     <variant>
        
        :return     <bool>
        """
        return isinstance(obj, Join)

