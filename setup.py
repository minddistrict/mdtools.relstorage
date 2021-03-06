from setuptools import setup, find_packages

version = '2.4.dev0'

long_description = (open('CHANGES.txt').read())

setup(
    name="mdtools.relstorage",
    description="Tools for ZODB/Relstorage",
    long_description=long_description,
    version=version,
    packages=find_packages('src'),
    include_package_data=True,
    namespace_packages=['mdtools'],
    zip_safe=False,
    package_dir={'': 'src'},
    install_requires=[
        'Relstorage',
        'python-dateutil',
        'zodbupdate >= 1.0',
        'ZODB',
        'ZEO',
        'setuptools',
        'zope.interface',
        ],
    entry_points={
        'console_scripts': [
            'relcheck = mdtools.relstorage.check:relstorage_main',
            'relsearch = mdtools.relstorage.search:relstorage_main',
            'relupdate = mdtools.relstorage.update:relstorage_main',
            'sqlpack = mdtools.relstorage.sqlpack:main',
            'zodbcheck = mdtools.relstorage.check:zodb_main',
            'zodblinks = mdtools.relstorage.links:main',
            'zodbsearch = mdtools.relstorage.search:zodb_main',
        ],
    },
)
