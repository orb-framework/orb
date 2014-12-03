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
    name = 'projex_orb',
    version = VERSION,
    author = 'Projex Software',
    author_email = 'team@projexsoftware.com',
    maintainer = 'Projex Software',
    maintainer_email = 'team@projexsoftware.com',
    description = 'Database ORM and API builder.',
    license = 'LGPL',
    keywords = '',
    url = '',
    include_package_data=True,
    packages = find_packages(),
    install_requires = REQUIREMENTS,
    tests_require = REQUIREMENTS,
    long_description= README,
    classifiers=[],
)