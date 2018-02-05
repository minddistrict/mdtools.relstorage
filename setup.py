from setuptools import setup, find_packages

version = '1.1.dev0'

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
        'zodbupdate',
        'ZODB',
        'ZEO',
        'setuptools',
        'zope.interface',
        ],
    entry_points={
        'console_scripts': [
            'relsearch = mdtools.relstorage.search:relstorage_main',
            'relupdate = mdtools.relstorage.update:relstorage_main',
            'sqlpack = mdtools.relstorage.sqlpack:main',
            'zodbcheck = mdtools.relstorage.check:zodb_main',
            'zodblinks = mdtools.relstorage.links:main',
            'zodbsearch = mdtools.relstorage.search:zodb_main',
        ],
    },
)
