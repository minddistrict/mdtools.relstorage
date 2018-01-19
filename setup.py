import os
from setuptools import setup, find_packages

version = '1.0'

long_description = (open('CHANGES.txt').read())

setup(
    name="mdtools.relstorage",
    description="Tools for Relstorage",
    long_description=long_description,
    version=version,
    packages=find_packages('src'),
    include_package_data=True,
    namespace_packages=['mdtools'],
    zip_safe=False,
    package_dir={'': 'src'},
    install_requires=[
        'Relstorage',
        'ZODB',
        'ZEO',
        'setuptools',
        'zope.interface',
        ],
    entry_points={
        'console_scripts': [
            'zodbcheck = mdtools.relstorage.check:main',
            'zodbsearch = mdtools.relstorage.search:main',
            'zodblinks = mdtools.relstorage.links:main',
            'sqlpack = mdtools.relstorage.sqlpack:main',
        ],
    },
)
