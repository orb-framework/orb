"""
Defines the base SQL class used for rendering SQL statements
out.
"""

import logging
import mako
import mako.template
import os
import orb
import sys

from projex.addon import AddonManager

log = logging.getLogger(__name__)


class SQL(AddonManager):
    def __init__(self, sql, baseSQL=None):
        super(SQL, self).__init__()
        
        # define custom properties
        self._template = mako.template.Template(sql, strict_undefined=True)
        self._sql = sql
        self._baseSQL = baseSQL or SQL

    def __call__(self, *args, **options):
        """
        Executes this statement with the inputted keywords to generate
        the context SQL statement.
        
        :param      **options | <keywords>
        
        :sa         render
        
        :return     <str> sql, <dict> data
        """
        # noinspection PyArgumentList
        return self.render(*args, **options)

    def baseSQL(self):
        """
        Returns the base SQL type to use for this instance.
        
        :return     subclass of <SQL>
        """
        return self._baseSQL

    def render(self, **scope):
        """
        Executes this statement with the inputted keywords to generate
        the context SQL statement.  Any keywords provided to the render
        method will be used as scope variables within the mako template for
        this SQL class.
        
        :param      **scope | <keywords>

        :return     <str> sql, <dict> data
        """
        # define common properties
        scope.setdefault('orb', orb)
        scope.setdefault('SQL', self.baseSQL())
        scope.setdefault('QUOTE', scope['SQL'].byName('QUOTE'))
        scope.setdefault('IO', {})
        scope.setdefault('GLOBALS', {})

        text = self._template.render(**scope)
        return text.strip()

    def setSQL(self, sql):
        """
        Sets the SQL mako statement for this instance.  This will generate
        a new mako Template that will be used when executing this command
        during generation.
        
        :param      sql | <str>
        """
        self._sql = sql
        self._template = mako.template.Template(sql)

    def sql(self):
        """
        Returns the template for this statement.
        
        :return     <str>
        """
        return self._sql

    @classmethod
    def createDatastore(cls):
        """
        Creates a new datastore instance for this sql class.
        
        :return     <orb.DataStore>
        """
        return orb.DataStore()

    @classmethod
    def datastore(cls):
        """
        Returns the base data store class for this SQL definition.
        
        :return     subclass of <orb.DataStore>
        """
        key = '_{0}__datastore'.format(cls.__name__)
        try:
            return getattr(cls, key)
        except AttributeError:
            store = cls.createDatastore()
            setattr(cls, key, store)
            return store

    @classmethod
    def loadStatements(cls, module):
        """
        Loads the mako definitions for the inputted name.  This is the inputted
        module that will be attempting to access the file.  When running
        with mako file support, this will read and load the mako file, when 
        built it will load a _mako.py module that defines the TEMPLATE variable
        as a string.
        
        :param      name | <str>
        
        :return     <str>
        """
        # load the shared statements
        from orb.core.backends.sql.shared import sql as shared_sql

        # load from the built table of contents
        if hasattr(module, '__toc__') and module.__toc__:
            mako_mods = module.__toc__
            for mako_mod in mako_mods:
                try:
                    __import__(mako_mod)
                    templ = sys.modules[mako_mod].TEMPLATE
                except StandardError:
                    log.error('Failed to load mako file: {0}'.format(mako_mod))
                    continue
                else:
                    name = mako_mod.split('.')[-1].replace('_sql_mako', '').upper()
                    typ = getattr(shared_sql, name, SQL)
                    cls.registerAddon(name, typ(templ, cls))
        
        # load from the directory
        else:
            base = os.path.dirname(module.__file__)
            files = os.listdir(os.path.dirname(module.__file__))
            for filename in files:
                if not filename.endswith('.mako'):
                    continue
                
                with open(os.path.join(base, filename), 'r') as f:
                    templ = f.read()
                
                name = filename.split('.')[0].upper()
                typ = getattr(shared_sql, name, SQL)
                cls.registerAddon(name, typ(templ, cls))

