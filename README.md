# mdtools.relstorage

`mdtools.relstorage` is a collection of tools to use with history-free
databases and relstorage which use PostgreSQL. Those tools only work
on *history-free* databases. If you try to use them on an another type
of database you will most likely break your database, lose
information, or have incorrect result so *please do not*.

Those tools are used with a database that uses blobs, but store them
on the filesystem and not in relstorage.

There are two families of tools:

- Tools starting with `zodb` which connects though a configuration
  file to a ZODB-based database and use the ZODB api. Those tools
  work slowly and does not support any parallelisation.

- Tools starting with `rel` which connects directly to a PostgreSQL
  database and work on directly on the records stored there by
  relstorage. By skipping the ZODB api, those tools are fast and
  support parallelisation.

## `zodbcheck` and `relcheck`

Those two tools build an sqlite database that contains all references
between objects. It will verify there are no missing objects (that
would generate `POSKey` errors in your application).

This database can then be used by:

- `sqlpack` to prepare an offline pack,

- `zodblinks` to expore the relations between objects.

## `zodbsearch` and `relsearch`

Those two tools can be used to load all objects stored in the database
and search for specific Python classes used by those objects. It will
report as well any missing Python class that is used but could not be
found.

The goal of this tool is to help you refactor your code without breaking
existing databases.

## `relupdate`

*You should not use this tool while your application is running or you
will lose data*

This tool is the relstorage variant of
[zodbupdate](https://pypi.org/project/zodbupdate) and can be used to
migrate a relstorage database from Python 2 to Python 3 faster than
using the original tool.

## `sqlpack`

This tool uses the database created by `zodbcheck` or `relcheck` and a
manifest of blobs and will create:

- A collection of SQL scripts to delete every object that are no
  longer in used from your relstorage database,

- A shell script to delete the associated blobs.

The purpose is to prepare an 'offline' pack of the your production
database on a copy that can be applied to the your real production
database later reliably and without any performance impact.

## `zodblinks`

This tool uses the database created by `zodbcheck` or `relcheck` and
let you explore which object is linked to which other object. It
allows you to find the "path" between two objects, meaning which
sequence of objects references that link two objects together.

It helps you to find and identify bugs that would leave persistent
objects in the database when they should have been deleted.
