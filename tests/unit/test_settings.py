def test_basic_settings():
    from orb.settings import Settings

    # validate the default values
    settings = Settings()

    assert settings.default_locale == 'en_US'
    assert settings.default_page_size == '40'
    assert settings.max_cache_timeout == '86400000'  # 24 hours
    assert settings.max_connections == '10'
    assert settings.security_key == ''
    assert settings.server_timezone == 'US/Pacific'
    assert settings.worker_class == 'default'


def test_environment_based_settings():
    import os
    from orb.settings import Settings

    try:
        # setup the default values via environment
        os.environ['ORB_DEFAULT_LOCALE'] = 'fr_FR'
        os.environ['ORB_DEFAULT_PAGE_SIZE'] = '100'
        os.environ['ORB_MAX_CACHE_TIMEOUT'] = '100'
        os.environ['ORB_MAX_CONNECTIONS'] = '1'
        os.environ['ORB_SECURITY_KEY'] = '12345'
        os.environ['ORB_SERVER_TIMEZONE'] = 'US/Eastern'
        os.environ['ORB_WORKER_CLASS'] = 'gevent'

        settings = Settings()

        # validate the environment based settings
        assert settings.default_locale == 'fr_FR'
        assert settings.default_page_size == '100'
        assert settings.max_cache_timeout == '100'
        assert settings.max_connections == '1'
        assert settings.security_key == '12345'
        assert settings.server_timezone == 'US/Eastern'
        assert settings.worker_class == 'gevent'

    finally:
        del os.environ['ORB_DEFAULT_LOCALE']
        del os.environ['ORB_DEFAULT_PAGE_SIZE']
        del os.environ['ORB_MAX_CACHE_TIMEOUT']
        del os.environ['ORB_MAX_CONNECTIONS']
        del os.environ['ORB_SERVER_TIMEZONE']
        del os.environ['ORB_SECURITY_KEY']
        del os.environ['ORB_WORKER_CLASS']


def test_initialization_based_settings():
    import os
    from orb.settings import Settings

    try:
        # setup the default values via environment
        os.environ['ORB_DEFAULT_LOCALE'] = 'fr_FR'
        os.environ['ORB_DEFAULT_PAGE_SIZE'] = '100'
        os.environ['ORB_MAX_CACHE_TIMEOUT'] = '100'
        os.environ['ORB_MAX_CONNECTIONS'] = '1'
        os.environ['ORB_SECURITY_KEY'] = '12345'
        os.environ['ORB_SERVER_TIMEZONE'] = 'US/Eastern'
        os.environ['ORB_WORKER_CLASS'] = 'gevent'

        settings = Settings(
            default_locale='en_GB',
            default_page_size='1',
            max_cache_timeout='10',
            max_connections='2',
            security_key='54321',
            server_timezone='US/Central',
            worker_class=''
        )

        # validate the environment based settings
        assert settings.default_locale == 'en_GB'
        assert settings.default_page_size == '1'
        assert settings.max_cache_timeout == '10'
        assert settings.max_connections == '2'
        assert settings.security_key == '54321'
        assert settings.server_timezone == 'US/Central'
        assert settings.worker_class == ''

    finally:
        del os.environ['ORB_DEFAULT_LOCALE']
        del os.environ['ORB_DEFAULT_PAGE_SIZE']
        del os.environ['ORB_MAX_CACHE_TIMEOUT']
        del os.environ['ORB_MAX_CONNECTIONS']
        del os.environ['ORB_SERVER_TIMEZONE']
        del os.environ['ORB_SECURITY_KEY']
        del os.environ['ORB_WORKER_CLASS']