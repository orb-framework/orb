""" Defines common globals to use for the Orb system. """

import os


class Settings(object):
    Defaults = {
        'default_locale': 'en_US',
        'server_timezone': 'US/Pacific',
        'security_key': '',
        'raise_background_errors': 'True',
        'caching_enabled': 'False',
        'max_cache_timeout': str(1000 * 60 * 60 * 24), # 24 hours
        'default_page_size': '40',
        'syntax': 'standard'  # possible values include standard, PEP8
    }

    def __init__(self):
        self.__dict__.update(dict(self.Defaults.items()))
        self.__dict__.update({k.replace('ORB_', '').lower(): v for k, v in os.environ.items()})

