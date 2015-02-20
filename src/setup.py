import os
from setuptools import setup, find_packages
import orb

here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.md')) as f:
        README = f.read()
except IOError:
    README = orb.__doc__

try:
    VERSION = orb.__version__
except AttributeError:
    VERSION = '2.5'

try:
    REQUIREMENTS = orb.__depends__
except AttributeError:
    REQUIREMENTS = []

setup(
    name = 'orb-api',
    version = VERSION,
    author = 'Eric Hulser',
    author_email = 'eric.hulser@gmail.com',
    maintainer = 'Eric Hulser',
    maintainer_email = 'eric.hulser@gmail.com',
    description = 'Database ORM and API builder.',
    license = 'LGPL',
    keywords = '',
    url = 'https://github.com/ProjexSoftware/orb',
    include_package_data=True,
    packages = find_packages(),
    install_requires = REQUIREMENTS,
    tests_require = REQUIREMENTS,
    long_description= README,
    classifiers=[],
)