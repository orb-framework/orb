"""
Defines the Settings class object used to control common
default settings for an ORB system.
"""

import os


class Settings(object):
    def __init__(self,
                 default_locale=None,
                 default_page_size=None,
                 max_cache_timeout=None,
                 max_connections=None,
                 security_key=None,
                 server_timezone=None,
                 worker_class=None):

        self.default_locale = default_locale or os.environ.get('ORB_DEFAULT_LOCALE', 'en_US')
        self.default_page_size = default_page_size or int(os.environ.get('ORB_DEFAULT_PAGE_SIZE', 40))
        self.max_cache_timeout = max_cache_timeout or int(os.environ.get('ORB_MAX_CACHE_TIMEOUT',
                                                                         1000 * 60 * 60 * 24))  # 24 hrs in ms
        self.max_connections = max_connections or int(os.environ.get('ORB_MAX_CONNECTIONS', 10))
        self.security_key = security_key or os.environ.get('ORB_SECURITY_KEY', '')
        self.server_timezone = server_timezone or os.environ.get('ORB_SERVER_TIMEZONE', 'US/Pacific')
        self.worker_class = worker_class or os.environ.get('ORB_WORKER_CLASS', 'default')


