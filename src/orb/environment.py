#!/usr/bin/python

""" Defines the global environment information for managing databases across. \
    Multiple environments. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

import logging
from xml.etree  import ElementTree

import projex.text
from projex.text import nativestring
from orb.database           import Database

logger = logging.getLogger(__name__)

class Environment(object):
    _environments   = {}
    _current        = None
    
    def __init__( self, name='production', description='', referenced=False ):
        self._name          = name
        self._description   = description
        self._database      = None
        self._databases     = {}
        self._referenced    = referenced
        self._default       = False
    
    def clear( self ):
        """
        Clears this environment's information.
        """
        for db in self._databases.values():
            db.disconnect()
    
    def database( self, name = '' ):
        """
        Returns the database with the inputed name for this environment.  If \
        no database is specifically defined for this environment, then the \
        default entry will be returned from the database class.
        
        :sa     Database.find
        
        :return     <Database> || None
        """
        if ( name ):
            return self._databases.get(nativestring(name), Database.find(name))
        else:
            return self._database
    
    def databases( self ):
        """
        Returns a list of all the databases in this environment.
        
        :return     [<Database>, ..]
        """
        return self._databases.values()
    
    def description( self ):
        """
        Returns the description of this environment.
        
        :return     <str>
        """
        return self._description
    
    def defaultDatabase( self ):
        """
        Returns the database that is the default for this environment.
        
        :return     <Database> || None
        """
        from orb import Orb
        
        # use the default database from the list
        first = None
        for db in self._databases.values():
            if ( not first ):
                first = db
                
            if ( db.isDefault() ):
                return db
        
        # use the current database from the orb system
        db = Orb.instance().database()
        
        if ( not db ):
            db = first
        
        return db
        
    def isDefault( self ):
        """
        Returns whether or not this environment is the default environment.
        
        :return     <bool>
        """
        return self._default
    
    def isReferenced(self):
        """
        Returns whether or not this environment is referenced from a
        separate ORB file.
        
        :return     <bool>
        """
        return self._referenced
    
    def name( self ):
        """
        Returns the name of this environment.
        
        :return     <str>
        """
        return self._name
    
    def registerDatabase( self, database, active = False ):
        """
        Registers a particular database with this environment.
        
        :param      database | <Database>
        """
        self._databases[database.name()] = database
        
        if ( active or not self._database ):
            self._database = database
    
    def save( self, filename ):
        """
        Saves the environment out to the inputed filename.
        
        :param      filename | <str>
        """
        # create the orb information
        import orb
        
        elem = ElementTree.Element('orb')
        elem.set('version', orb.__version__)
        
        envs = ElementTree.SubElement(elem, 'environments')
        self.toXml(envs)
        
        projex.text.xmlindent(elem)
        env_file = open(filename, 'w')
        env_file.write(ElementTree.tostring(elem))
        env_file.close()
    
    def setCurrent( self ):
        """
        Sets this environment as the current database environment.
        
        :return     <bool> | changed
        """
        if ( self == Environment._current ):
            return False
        
        Environment._current = self
        return True
    
    def setDatabase( self, database ):
        """
        Sets the active database to the inputed database.
        
        :param      database | <orb.Database>
        """
        self._database = database
    
    def setDescription( self, description ):
        """
        Sets the description for this environment to the inputed description.
        
        :param      description | <str>
        """
        self._description = description
    
    def setDefault( self, state ):
        """
        Sets this environment to the default environment.
        
        :param      state | <bool>
        
        :return     <bool> | changed
        """
        if ( self._default == state ):
            return False
        
        self._default = state
        if ( state ):
            for env in Environment._environments.values():
                if ( not env.isDefault() ):
                    continue
                    
                env._default = False
                break
        
        return True
    
    def setName( self, name ):
        """
        Sets the name for this environment to the inputed name.
        
        :param      name | <str>
        """
        self._name = nativestring(name)
    
    def toXml( self, xparent ):
        """
        Converts this environment to XML data and returns it.
        
        :param      xparent | <xml.etree.ElementTree.Element>
        """
        xenv = ElementTree.SubElement(xparent, 'environment')
        xenv.set('name',    nativestring(self.name()))
        xenv.set('default', nativestring(self.isDefault()))
        ElementTree.SubElement(xenv, 'description').text = self.description()
        
        xdbs = ElementTree.SubElement(xenv, 'databases')
        for db in self._databases.values():
            db.toXml(xdbs)
        
        return xenv
    
    def unregisterDatabase( self, database ):
        """
        Un-registers a particular database with this environment.
        
        :param      database | <Database>
        """
        if ( database.name() in self._databases ):
            database.disconnect()
            self._databases.pop(database.name())
    
    @staticmethod
    def current():
        """
        Returns the current environment for the orb system.
        
        :return     <Environment> || None
        """
        return Environment._current
    
    @staticmethod
    def fromXml( xenv, referenced=False ):
        """
        Creates a new environment instance and returns it from the inputed \
        xml data.
        
        :param      xenv | <xml.etree.ElementTree.Element>
        """
        env = Environment(referenced=referenced)
        env.setName(xenv.get('name', ''))
        env.setDefault(xenv.get('default') == 'True')
        
        xdesc = xenv.find('description')
        if xdesc is not None:
            env.setDescription(xdesc.text)
        
        # load databases
        xdbs = xenv.find('databases')
        if xdbs is not None:
            for xdb in xdbs:
                db = Database.fromXml(xdb, referenced)
                env.registerDatabase(db)
        
        return env
    
    @staticmethod
    def load( filename ):
        """
        Loads the environments defined within the inputed filename.
        
        :param      filename        | <str>
        
        :return     <Environment> || None
        """
        try:
            xtree = ElementTree.parse(filename)
        except xml.parsers.expat.ExpatError:
            return None
            
        # create a new database
        return Environment.fromXml(xtree.getroot())
    
    @staticmethod
    def findDefault():
        """
        Returns the default environment, if any is set to default.
        
        :return     <Environment> || None
        """
        for env in Environment._environments.values():
            if ( env.isDefault() ):
                return env
        return None
    
    @staticmethod
    def find( name ):
        """
        Looks up an environment based on the inputed name.
        
        :return     <str>
        """
        return Environment._environments.get(nativestring(name))