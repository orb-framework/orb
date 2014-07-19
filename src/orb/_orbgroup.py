#!/usr/bin/python

""" Defines a grouping system for schemas. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2012, Projex Software'
__license__         = 'LGPL'

# maintenance information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import logging
import os.path
import sys

from projex.text import nativestring

from xml.etree import ElementTree

import projex

logger = logging.getLogger(__name__)

class OrbGroup(object):
    def __init__( self, name = '', referenced = False ):
        self._name          = name
        
        # reference information
        self._requires      = []
        self._module        = ''
        self._filename      = ''
        self._order         = 0
        self._referenced    = referenced
        
        self._databaseName  = ''
        self._namespace     = ''
        self._schemas       = set()
        self._properties    = {}
        self._modelPrefix   = ''
    
    def addSchema( self, schema ):
        """
        Adds the inputed schema to the group.
        
        :param      schema | <TableSchema>
        """
        self._schemas.add(schema)
    
    def database( self ):
        """
        Returns the database linked with this group.
        
        :return     <orb.Database> || None
        """
        from orb import Orb
        return Orb.instance().database(self.databaseName())
    
    def databaseName( self ):
        """
        Returns the default database name for this schema group.
        
        :return     <str>
        """
        return self._databaseName
    
    def filename( self ):
        """
        Returns the filename string for this instance.
        
        :return     <str>
        """
        return self._filename
    
    def isReference( self ):
        """
        Returns whether or not this group is refering to an external module.
        
        :return     <bool>
        """
        return self.isReferenced()
    
    def isReferenced(self):
        """
        Returns whether or not this group is referenced from a separate file.
        
        :return     <bool>
        """
        return self._referenced
    
    def merge( self ):
        """
        Merges the group from its reference information.
        """
        from orb import Orb
        
        modname = self.module()
        
        if ( not modname ):
            return 0
        
        requires = [modname.split('.')[0]] + self.requires()
        projex.requires(*requires)
        
        try:
            __import__(modname)
        except ImportError:
            logger.exception('Could not import: %s.' % modname)
            return 0
        
        module      = sys.modules[modname]
        basepath    = os.path.dirname(module.__file__)
        filename    = os.path.join(basepath, self.name().lower() + '.orb')
        
        try:
            xorb = ElementTree.parse(nativestring(filename)).getroot()
            
        except xml.parsers.expat.ExpatError:
            logger.exception('Failed to load ORB file: %s' % filename)
            return False
        
        # load schemas
        from orb import TableSchema
        
        count   = 0
        xgroups = xorb.find('groups')
        for xgroup in xgroups:
            if ( xgroup.get('name') != self.name() ):
                continue
            
            xschemas = xgroup.find('schemas')
            if ( xschemas is None ):
                return 0
            
            for xschema in xschemas:
                schema = TableSchema.fromXml(xschema)
                schema.setGroupName(self.name())
                schema.setDatabaseName(self.databaseName())
                self.addSchema(schema)
                count += 1
        
        return count
    
    def modelPrefix( self ):
        """
        Returns the string that will be used as the prefix for generating
        all models for a particular group.  This is useful when keeping
        typed objects together under a common namespace.
        
        :return     <str>
        """
        return self._modelPrefix
    
    def module( self ):
        """
        Returns the module that this group is coming from.
        
        :return     <str>
        """
        return self._module
    
    def name( self ):
        """
        Returns the name for this group.
        
        :return     <str>
        """
        return self._name
    
    def namespace( self ):
        """
        Returns the namespace for this group.  If no namespace is explictely
        defined, then the global Orb namespace is returned.
        
        :return     <str>
        """
        if ( self._namespace ):
            return self._namespace
        
        db = self.database()
        if ( db ):
            return db.namespace()
        
        from orb import Orb
        return Orb.instance().namespace()
    
    def order( self ):
        """
        Returns the order that this group should be loaded in.
        
        :return     <int>
        """
        return self._order
    
    def property( self, key, default = '' ):
        """
        Returns the property value for the inputed key string.
        
        :param      key | <str>
                    default | <str>
        
        :return     <str>
        """
        return self._properties.get(nativestring(key), nativestring(default))
    
    def removeSchema( self, schema ):
        """
        Removes the inputed schema from this group.
        
        :param      schema | <TableSchema>
        """
        if ( schema in self._schemas ):
            self._schemas.remove(schema)
    
    def requires( self ):
        """
        Returns the requirements to pass to the projex environment system \
        for this group.
        
        :return     [<str>, ..]
        """
        return self._requires
    
    def schemas( self ):
        """
        Returns a list of schemas that are linked with this group.
        
        :return     [<TableSchema>, ..]
        """
        return list(self._schemas)
    
    def setDatabaseName( self, name ):
        """
        Sets the default database name for this schema to the inputed name.
        
        :param      name | <str>
        """
        self._databaseName = name
    
    def setModelPrefix( self, prefix ):
        """
        Sets the string that will be used as the prefix for generating
        all models for a particular group.  This is useful when keeping
        typed objects together under a common namespace.
        
        :param      prefix | <str>
        """
        self._modelPrefix = prefix
    
    def setModule( self, module ):
        """
        Sets the module name for this group to the inputed module name.
        
        :param      module | <str>
        """
        self._module = module
    
    def setName( self, name ):
        """
        Sets the name for this group to the inputed name.
        
        :param      name | <str>
        """
        self._name = name
    
    def setFilename( self, filename ):
        """
        Sets the filename for this instance to the inputed filename.
        
        :param      filename | <str>
        """
        self._filename = filename
    
    def setNamespace( self, namespace ):
        """
        Sets the namespace that will be used for this system.
        
        :param      namespace | <str>
        """
        self._namespace = namespace
    
    def setOrder( self, order ):
        """
        Sets the order that this group should be loaded in.
        
        :param      order | <int>
        """
        self._order = order
    
    def setProperty( self, key, value ):
        """
        Sets the property value for the given key, value pairing.
        
        :param      key | <str>
                    value | <str>
        """
        self._properties[nativestring(key)] = nativestring(value)
    
    def setRequires( self, requires ):
        """
        Sets the requirements for this system to the inputed modules.
        
        :param      requires | [<str>, ..]
        """
        self._requires = requires[:]
    
    def toXml(self, xparent):
        """
        Saves the schema group to the inputed xml.
        
        :param      xparent | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xgroup = ElementTree.SubElement(xparent, 'group')
        xgroup.set('name', self.name())
        if self.isReferenced():
            xgroup.set('referenced', 'True')
        else:
            xgroup.set('dbname', self.databaseName())
            xgroup.set('prefix', self.modelPrefix())
            xgroup.set('namespace', self._namespace)
            
            # save the properties
            xprops = ElementTree.SubElement(xgroup, 'properties')
            for key, value in self._properties.items():
                xprop = ElementTree.SubElement(xprops, 'property')
                xprop.set('key', key)
                xprop.set('value', value)
            
            # save reference information
            if self.module():
                xgroup.set('module', self.module())
                xgroup.set('requires', ','.join(self.requires()))
        
        if not self.module():
            xschemas = ElementTree.SubElement(xgroup, 'schemas')
            for schema in sorted(self.schemas(), key=lambda x: x.name()):
                if not schema.isReferenced():
                    schema.toXml(xschemas)
        
        return xgroup
    
    @staticmethod
    def fromXml( xgroup, referenced=False, database=None ):
        """
        Loads the schema group from the inputed xml schema data.
        
        :param      xgroup      | <xml.etree.ElementTree.Element>
                    referenced  | <bool>
                    database    | <str> || None
        
        :return     (<OrbGroup>, [<orb.TableSchema>, ..]) || (None, [])
        """
        from orb import TableSchema, Orb
        
        # load schemas
        grpname = xgroup.get('name')
        dbname  = xgroup.get('dbname')
        modname = xgroup.get('module')
        
        # force database to import
        if database is not None:
            dbname = database
        
        # import a reference file
        if modname:
            requires = xgroup.get('requires', '').split(',')
            while ( '' in requires ):
                requires.remove('')
            
            projex.requires(*requires)
            
            try:
                __import__(modname)
            except ImportError:
                logger.exception('Error importing group plugin: %s' % modname)
                return (None, [])
            
            grp = Orb.instance().group(grpname, database=dbname)
            if not grp:
                return (None, [])
                
            grp.setDatabaseName(dbname)
            grp.setModule(modname)
            grp.setRequires(requires)
            
            # load properties
            xprops = xgroup.find('properties')
            if xprops is not None:
                for xprop in xprops:
                    grp.setProperty(xprop.get('key'), xprop.get('value'))
            
            return (None, [])
        
        # import non-referenced schemas
        else:
            grp = Orb.instance().group(grpname, database=dbname)
            if not grp:
                grp = OrbGroup(referenced=referenced)
                grp.setName(grpname)
                grp.setModelPrefix(xgroup.get('prefix', ''))
                grp.setNamespace(xgroup.get('namespace', ''))
                if dbname is not None:
                    grp.setDatabaseName(dbname)
            
            # load schemas
            schemas = []
            xschemas = xgroup.find('schemas')
            if xschemas is not None:
                for xschema in xschemas:
                    schema = TableSchema.fromXml(xschema, referenced)
                    schema.setGroup(grp)
                    grp.addSchema(schema)
                    schemas.append(schema)
                    
                    if dbname is not None:
                        schema.setDatabaseName(dbname)
            
            # load properties
            xprops = xgroup.find('properties')
            if xprops is not None:
                for xprop in xprops:
                    grp.setProperty(xprop.get('key'), xprop.get('value'))
            
            return (grp, schemas)