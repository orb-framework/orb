from setuptools import setup, find_packages

setup(
    name='orb-api',
    version='4.4.17',
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
    install_requires=[
        'projex',
        'mako'
    ],
    tests_require=[],
    long_description='Database ORM and API builder.',
    classifiers=[],
)