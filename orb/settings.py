""" Defines common globals to use for the Orb system. """

import os


class Settings(object):
    def __init__(self):
        env = os.environ
        
        # define custom properties
        self._defaultTimezone = env.get('ORB_TIMEZONE', '')
        self._raiseBackendErrors = env.get('ORB_RAISE_BACKEND_ERRORS') != 'False'
        self._cachingEnabled = env.get('ORB_CACHING_ENABLED') != 'False'
        self._maxCacheTimeout = int(env.get('ORB_MAX_CACHE_TIMEOUT', 60 * 24))
        self._editOnlyMode = env.get('ORB_EDIT_ONLY_MODE') == 'True'
        self._optimizeDefaultEmpty = env.get('ORB_OPTIMIZE_DEFAULT_EMPTY') == 'True'
        self._defaultPageSize = int(env.get('ORB_DEFAULT_PAGE_SIZE', '40'))
        
        # setup field options (version 1)
        if env.get('ORB_CONFIG_VERSION') == '1':
            self._primaryName = env.get('ORB_PRIMARY_NAME', '_id')
            self._primaryField = env.get('ORB_PRIMARY_FIELD', '_id')
            self._primaryGetter = env.get('ORB_PRIMARY_GETTER', '_id')
            self._primarySetter = env.get('ORB_PRIMARY_SETTER', '_setId')
            self._primaryDisplay = env.get('ORB_PRIMARY_DISPLAY', 'Id')
            self._primaryIndex = env.get('ORB_PRIMARY_INDEX', 'byId')
            
            self._inheritField = env.get('ORB_INHERIT_FIELD', '__inherits__')
        
        # use latest field options
        else:
            self._primaryName = env.get('ORB_PRIMARY_NAME', 'id')
            self._primaryField = env.get('ORB_PRIMARY_FIELD', 'id')
            self._primaryGetter = env.get('ORB_PRIMARY_GETTER', 'id')
            self._primarySetter = env.get('ORB_PRIMARY_SETTER', 'setId')
            self._primaryDisplay = env.get('ORB_PRIMARY_INDEX', 'Id')
            self._primaryIndex = env.get('ORB_PRIMARY_INDEX', 'byId')
            self._inheritField = env.get('ORB_INHERIT_FIELD', '{table}_id__baseref')

    def editOnlyMode(self):
        """
        Returns if this object should be in edit only mode.
        
        :return     <bool>
        """
        return self._editOnlyMode

    def defaultPageSize(self):
        """
        Returns the default page size that will be used within the paging system
        on the <orb.RecordSet> class.
        
        :return     <int>
        """
        return self._defaultPageSize

    def defaultTimezone(self):
        """
        Returns the default timezone from the settings.
        
        :return     <str>
        """
        return self._defaultTimezone

    def inheritField(self):
        """
        Returns the default inherit field for the primary field.
        
        :return     <str>
        """
        return self._inheritField

    def isCachingEnabled(self):
        """
        Returns whether or not global caching is enabled.
        
        :return     <bool>
        """
        return self._cachingEnabled

    def maxCacheTimeout(self):
        """
        Returns the maximum timeout for the system to hold cached results
        in minutes.
        
        :return     <int> | minutes
        """
        return self._maxCacheTimeout

    def optimizeDefaultEmpty(self):
        """
        Returns whether or not the settings should optimize on default
        empty.
        
        :return     <bool>
        """
        return self._optimizeDefaultEmpty

    def primaryDisplay(self):
        """
        Returns the default display name for the primary field.
        
        :return     <str>
        """
        return self._primaryDisplay

    def primaryField(self):
        """
        Returns the default field name for the primary field.
        
        :return     <str>
        """
        return self._primaryField

    def primaryGetter(self):
        """
        Returns the default getter name for the primary field.
        
        :return     <str>
        """
        return self._primaryGetter

    def primaryIndex(self):
        """
        Returns the default index name for the primary field.
        
        :return     <str>
        """
        return self._primaryIndex

    def primaryName(self):
        """
        Returns the default name for the primary field.

        :return     <str>
        """
        return self._primaryName

    def primarySetter(self):
        """
        Returns the default setter name for the primary field.
        
        :return     <str>
        """
        return self._primarySetter

    def setCachingEnabled(self, state=True):
        """
        Sets whether or not global caching is enabled.
        
        :param      state | <bool>
        """
        self._cachingEnabled = state

    def setDefaultPageSize(self, pageSize):
        """
        Sets the default page size that will be used within the paging system
        on the <orb.RecordSet> class.
        
        :param      pageSize | <int>
        """
        self._defaultPageSize = pageSize

    def setDefaultTimezone(self, timezone):
        """
        Sets the default timezone for these settings.
        
        :param      timezone | <str>
        """
        self._defaultTimezone = timezone

    def setInheritField(self, field):
        """
        Sets the default inherit field for the primary field.
        
        :param      field | <str>
        """
        self._inheritField = field

    def setMaxCacheTimeout(self, minutes):
        """
        Sets the maximum timeout for the system to hold cached results
        in minutes.
        
        :param      minutes | <int>
        """
        self._maxCacheTimeout = minutes

    def setPrimaryDisplay(self, text):
        """
        Sets the default display name for the primary field.
        
        :param      text | <str>
        """
        self._primaryDisplay = text

    def setPrimaryField(self, text):
        """
        Sets the default field name for the primary field.
        
        :param      text | <str>
        """
        self._primaryField = text

    def setPrimaryGetter(self, text):
        """
        Sets the default getter name for the primary field.
        
        :param      text | <str>
        """
        self._primaryGetter = text

    def setPrimaryIndex(self, text):
        """
        Sets the default index name for the primary field.
        
        :param      text | <str>
        """
        self._primaryIndex = text

    def setPrimaryName(self, text):
        """
        Sets the default name for the primary field.

        :param      text | <str>
        """
        self._primaryName = text

    def setPrimarySetter(self, text):
        """
        Sets the default setter name for the primary field.
        
        :param      text | <str>
        """
        self._primarySetter = text

    def setRaiseBackendErrors(self, state=True):
        """
        Sets whether or not backend errors will be raised for the system.
        
        :param      state | <bool>
        """
        self._raiseBackendErrors = state

    def raiseBackendErrors(self):
        """
        Returns whether or not backend errors will be raised when they
        are hit.
        
        :return     <bool>
        """
        return self._raiseBackendErrors

