import os
import re
import subprocess

from setuptools import setup, find_packages, Command
from setuptools.command.test import test as TestCommand

__author__ = 'Eric Hulser'
__email__ = 'eric.hulser@gmail.com'
__license__ = 'MIT'

INSTALL_REQUIRES = []
DEPENDENCY_LINKS = []
TESTS_REQUIRE = []
LONG_DESCRIPTION = ''


class Tox(TestCommand):
    def run_tests(self):
        import tox
        tox.cmdline()


class MakeDocs(Command):
    description = 'Generates documentation'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system('pip install -r requirements-dev.txt')
        os.system('sphinx-apidoc -f -o docs/source/api orb')
        os.system('sphinx-build -b html docs/source docs/build')


class Release(Command):
    description = 'Runs the tests and releases a new version of the script'
    user_options = [
        ('no-tests', None, 'Bypass the test validation before releasing')
    ]

    def initialize_options(self):
        self.no_tests = True  # for now, default this to true...

    def finalize_options(self):
        pass

    def run(self):
        if self.no_tests:
            print('[WARNING] No tests have been run for this release!')

        if not self.no_tests and os.system('python setup.py test'):
            print('[ERROR] Could not release, tests are failing!')
        else:
            os.system('python setup.py tag')
            os.system('python setup.py bdist_wheel bdist_egg upload')


class Tag(Command):
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
            f.write('__revision__ = "{0}"\n'.format(result.group(3)))
            f.write('__hash__ = "{0}"'.format(result.group(4)))

        # tag this new release version
        if not self.no_tag:
            version = '.'.join([result.group(1), result.group(2), result.group(3)])

            print 'creating git tag:', 'v' + version

            os.system('git tag -a v{0} -m "releasing {0}"'.format(version))
            os.system('git push --tags')
        else:
            print 'warning: tagging ignored...'


def read_requirements_file(path):
    """
    reads requirements.txt file and handles PyPI index URLs
    :param path: (str) path to requirements.txt file
    :return: (tuple of lists)
    """
    last_pypi_url = None
    with open(path) as f:
        requires = []
        pypi_urls = []
        for line in f.readlines():
            if not line:
                continue
            if '--' in line:
                match = re.match(r'--index-url\s+([\w\d:/.-]+)\s', line)
                if match:
                    last_pypi_url = match.group(1)
                    if not last_pypi_url.endswith("/"):
                        last_pypi_url += "/"
            else:
                if last_pypi_url:
                    pypi_urls.append(last_pypi_url + line.strip().lower())
                requires.append(line)
    return requires, pypi_urls


if __name__ == '__main__':
    try:
        with open('orb/_version.py', 'r') as f:
            content = f.read()
            major = re.search('__major__ = (\d+)', content).group(1)
            minor = re.search('__minor__ = (\d+)', content).group(1)
            rev = re.search('__revision__ = "([^"]+)"', content).group(1)
            VERSION = '.'.join((major, minor, rev))
    except StandardError:
        VERSION = '0.0.0'

    # parse the requirements file
    if os.path.isfile('requirements.txt'):
        _install_requires, _pypi_urls = read_requirements_file('requirements.txt')
        INSTALL_REQUIRES.extend(_install_requires)
        DEPENDENCY_LINKS.extend(_pypi_urls)

    if os.path.isfile('tests/requirements.txt'):
        _tests_require, _pypi_urls = read_requirements_file('tests/requirements.txt')
        TESTS_REQUIRE.extend(_tests_require)
        DEPENDENCY_LINKS.extend(_pypi_urls)

    # Get the long description from the relevant file
    if os.path.isfile('README.md'):
        with open('README.md') as f:
            LONG_DESCRIPTION = f.read()

    setup(
        name='orb-api',
        version=VERSION,
        author=__author__,
        author_email=__email__,
        maintainer=__author__,
        maintainer_email=__email__,
        description='Database ORM and API builder.',
        license=__license__,
        keywords='',
        url='https://github.com/orb-framework/orb',
        install_requires=INSTALL_REQUIRES,
        packages=find_packages(),
        tests_require=TESTS_REQUIRE,
        test_suite='tests',
        long_description=LONG_DESCRIPTION,
        cmdclass={
            'tag': Tag,
            'release': Release,
            'mkdocs': MakeDocs,
            'test': Tox
        }
    )