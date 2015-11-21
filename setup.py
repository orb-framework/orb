import os
import re
import subprocess
import sys

from setuptools import setup, find_packages, Command
from setuptools.command.test import test as TestCommand

try:
    with open('orb/_version.py', 'r') as f:
        content = f.read()
        major = re.search('__major__ = (\d+)', content).group(1)
        minor = re.search('__minor__ = (\d+)', content).group(1)
        rev = re.search('__revision__ = (\d+)', content).group(1)
        version = '.'.join((major, minor, rev))
except StandardError:
     version = '0.0.0'


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to py.test')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.default_options = ['tests/']
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.default_options)
        sys.exit(errno)


class tag(Command):
    description = 'Command used to release new versions of the website to the internal pypi server.'
    user_options = [
        ('no-tag', None, 'Do not tag the repo before releasing')
    ]

    def initialize_options(self):
        self.no_tag = False

    def finalize_options(self):
        pass

    def run(self):
        # generate the version information from the current git commit
        cmd = ['git', 'describe', '--match', 'v[0-9]*.[0-9]*.0']
        desc = subprocess.check_output(cmd).strip()
        result = re.match('v([0-9]+)\.([0-9]+)\.0-([0-9]+)-(.*)', desc)

        print 'generating version information from:', desc
        with open('./orb/_version.py', 'w') as f:
            f.write('__major__ = {0}\n'.format(result.group(1)))
            f.write('__minor__ = {0}\n'.format(result.group(2)))
            f.write('__revision__ = "dev{0}"\n'.format(result.group(3)))
            f.write('__hash__ = "{0}"'.format(result.group(4)))

        # tag this new release version
        if not self.no_tag:
            version = '.'.join([result.group(1), result.group(2), 'dev' + result.group(3)])

            print 'creating git tag:', 'v' + version

            os.system('git tag -a v{0} -m "releasing {0}"'.format(version))
            os.system('git push --tags')
        else:
            print 'warning: tagging ignored...'

setup(
    name='orb-api',
    version=version,
    author='Eric Hulser',
    author_email='eric.hulser@gmail.com',
    maintainer='Eric Hulser',
    maintainer_email='eric.hulser@gmail.com',
    description='Database ORM and API builder.',
    license='LGPL',
    keywords='',
    url='https://github.com/ProjexSoftware/orb',
    include_package_data=True,
    packages=find_packages(),
    cmdclass={
        'tag': tag,
        'test': PyTest
    },
    tests_require=[],
    long_description='Database ORM and API builder.',
    classifiers=[],
)