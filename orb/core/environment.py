""" Defines the global environment information for managing databases across. \
    Multiple environments. """

import projex.text
import xml.parsers.expat

from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree

orb = lazy_import('orb')


class Environment(object):
    def __init__(self,
                 name='production',
                 description='',
                 referenced=False,
                 manager=None):
        self._name = name
        self._description = description
        self._database = None
        self._databases = {}
        self._referenced = referenced
        self._default = False
        self._manager = manager

    def clear(self):
        """
        Clears this environment's information.
        """
        for db in self._databases.values():
            db.disconnect()

    def database(self, name=''):
        """
        Returns the database with the inputted name for this environment.  If \
        no database is specifically defined for this environment, then the \
        default entry will be returned from the database class.
        
        :sa     <orb.Database.byName>
        
        :return     <orb.Database> || None
        """
        if name:
            db = orb.Database.byName(name)
            return self._databases.get(nstr(name), db)
        else:
            return self._database

    def databases(self):
        """
        Returns a list of all the databases in this environment.
        
        :return     [<orb.Database>, ..]
        """
        return self._databases.values()

    def description(self):
        """
        Returns the description of this environment.
        
        :return     <str>
        """
        return self._description

    def defaultDatabase(self):
        """
        Returns the database that is the default for this environment.
        
        :return     <orb.Database> || None
        """
        # use the default database from the list
        first = None
        for db in self._databases.values():
            if not first:
                first = db

            if db.isDefault():
                return db

        # use the current database from the orb system
        db = orb.system.database()
        if not db:
            db = first
        return db

    def isDefault(self):
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

    def manager(self):
        """
        Returns the manager associated with this environment.  If
        no specific manager has been assigned, than the global orb
        system manager will be returned.
        
        :return     <orb.Manager>
        """
        if not self._manager:
            return orb.system
        else:
            return self._manager

    def name(self):
        """
        Returns the name of this environment.
        
        :return     <str>
        """
        return self._name

    def registerDatabase(self, database, active=False):
        """
        Registers a particular database with this environment.
        
        :param      database | <orb.Database>
        """
        self._databases[database.name()] = database

        if active or not self._database:
            self._database = database

    def save(self, filename):
        """
        Saves the environment out to the inputted filename.
        
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

    def setCurrent(self):
        """
        Sets this environment as the current database environment.
        
        :return     <bool> | changed
        """
        self.manager().setEnvironment(self)

    def setDatabase(self, database):
        """
        Sets the active database to the inputted database.
        
        :param      database | <orb.Database>
        """
        self._database = database

    def setDescription(self, description):
        """
        Sets the description for this environment to the inputted description.
        
        :param      description | <str>
        """
        self._description = description

    def setDefault(self, state):
        """
        Sets this environment to the default environment.
        
        :param      state | <bool>
        
        :return     <bool> | changed
        """
        if self._default == state:
            return False

        self._default = state
        if state:
            for env in self.manager().environments():
                if not env.isDefault():
                    continue

                env._default = False
                break

        return True

    def setName(self, name):
        """
        Sets the name for this environment to the inputted name.
        
        :param      name | <str>
        """
        self._name = nstr(name)

    def toXml(self, xparent):
        """
        Converts this environment to XML data and returns it.
        
        :param      xparent | <xml.etree.ElementTree.Element>
        """
        xenv = ElementTree.SubElement(xparent, 'environment')
        xenv.set('name', nstr(self.name()))
        xenv.set('default', nstr(self.isDefault()))
        ElementTree.SubElement(xenv, 'description').text = self.description()

        xdbs = ElementTree.SubElement(xenv, 'databases')
        for db in self._databases.values():
            db.toXml(xdbs)

        return xenv

    def unregisterDatabase(self, database):
        """
        Un-registers a particular database with this environment.
        
        :param      database | <orb.Database>
        """
        if database.name() in self._databases:
            database.disconnect()
            self._databases.pop(database.name())

    @staticmethod
    def current(manager=None):
        """
        Returns the current environment for the orb system.
        
        :param      manager | <orb.Manager> || None
        
        :return     <orb.Environment> || None
        """
        if manager is None:
            manager = orb.system
        return manager.database()

    @staticmethod
    def fromXml(xenv, referenced=False):
        """
        Creates a new environment instance and returns it from the inputted \
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
                db = orb.Database.fromXml(xdb, referenced)
                env.registerDatabase(db)

        return env

    @staticmethod
    def load(filename):
        """
        Loads the environments defined within the inputted filename.
        
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
    def findDefault(manager=None):
        """
        Returns the default environment, if any is set to default.
        
        :param      manager | <orb.Manager> || None
        
        :return     <orb.Environment> || None
        """
        if manager is None:
            manager = orb.system

        for env in manager.environments():
            if env.isDefault():
                return env
        return None

