import pytest


def test_create_new_system():
    from orb.core.system import System

    system = System()
    assert system.database() is None
    assert system.schemas() == {}

    assert system.settings.default_locale == 'en_US'
    assert system.settings.server_timezone == 'US/Pacific'
    assert system.settings.max_connections == 10


def test_create_new_system_with_custom_settings():
    from orb.core.system import System

    system = System(locale='fr_FR', max_connections=20)
    assert system.database() is None
    assert system.schemas() == {}

    assert system.settings.default_locale == 'fr_FR'
    assert system.settings.server_timezone == 'US/Pacific'
    assert system.settings.max_connections == 20