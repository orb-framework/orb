"""
Defines the Settings class object used to control common
default settings for an ORB system.
"""

import os


class Settings(object):
    Defaults = {
        'default_locale': 'en_US',
        'default_page_size': '40',
        'max_cache_timeout': str(1000 * 60 * 60 * 24),  # 24 hours
        'max_connections': '10',
        'security_key': '',
        'server_timezone': 'US/Pacific',
        'worker_class': 'default'
    }

    def __init__(self, **kw):
        data = Settings.Defaults.copy()
        data.update({k.replace('ORB_', '').lower(): v for k, v in os.environ.items()})
        data.update({k: v for k, v in kw.items() if k in Settings.Defaults})

        # assign the settings properties
        self.__dict__.update(data)

