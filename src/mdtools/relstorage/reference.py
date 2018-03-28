import logging
import sqlite3
import sys

import ZODB.utils
import zope.interface
import mdtools.relstorage.interfaces

COMMIT_EVERY = 50000

logger = logging.getLogger('mdtools.relstorage.reference')


def connect(journal=True):
    """Decorator for the reference database to access the sqlite DB."""

    def decorator(callback):

        def wrapper(self, *args, **kwargs):
            try:
                connection = sqlite3.connect(self.db_name)
            except Exception:
                raise ValueError(
                    'impossible to open references database {}.'.format(
                        self.db_name))
            cursor = connection.cursor()
            cursor.execute("PRAGMA cache_size=25000")
            if not journal:
                cursor.execute("PRAGMA journal_mode=OFF")
                connection.isolation_level = None
            connection.commit()
            try:
                result = callback(self, connection, *args, **kwargs)
            finally:
                connection.close()
            return result

        return wrapper

    return decorator


@zope.interface.implementer(mdtools.relstorage.interfaces.IDatabase)
class Database(object):

    def __init__(self, db_name):
        self.db_name = db_name

    @connect(journal=False)
    def add_references(self, connection, records):
        cursor = connection.cursor()
        for source_oid, target_oids in records:
            for target_oid in target_oids or [-1]:
                cursor.execute("""
INSERT INTO links (source_oid, target_oid) VALUES
(?, ?)
            """, (source_oid, target_oid))

    @connect()
    def create_database(self, connection):
        logger.info('Creating reference database')
        cursor = connection.cursor()
        cursor.execute("PRAGMA page_size=16384")
        cursor.execute("VACUUM")
        connection.commit()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA main.locking_mode=EXCLUSIVE")
        cursor.execute("PRAGMA main.synchronous=OFF")
        cursor.execute("""
CREATE TABLE IF NOT EXISTS links
(source_oid BIGINT, target_oid BIGINT)
        """)
        connection.commit()

    @connect(journal=False)
    def finish_database(self, connection):
        logger.info('Indexing reference database')
        cursor = connection.cursor()
        cursor.execute("""
CREATE INDEX IF NOT EXISTS source_oid_index ON links (source_oid)
        """)
        cursor.execute("""
CREATE INDEX IF NOT EXISTS target_oid_index ON links (target_oid)
        """)
        cursor.execute("PRAGMA main.locking_mode=NORMAL")
        cursor.execute("PRAGMA main.synchronous=NORMAL")
        cursor.execute("PRAGMA optimize")

    @connect()
    def check_database(self, connection):
        cursor = connection.cursor()
        try:
            result = cursor.execute("SELECT count(*) FROM links")
            result.fetchall()
        except sqlite3.OperationalError:
            return False
        return True

    @connect()
    def get_unused_oids(self, connection):
        oids = set([])
        cursor = connection.cursor()
        result = cursor.execute("""
WITH RECURSIVE links_to_root (source_oid, target_oid) AS (
    SELECT source_oid, target_oid
    FROM links
    WHERE source_oid = 0
    UNION
    SELECT linked.source_oid, linked.target_oid
    FROM links AS linked JOIN links_to_root AS origin
        ON origin.target_oid = linked.source_oid
    ORDER BY linked.source_oid
)
SELECT DISTINCT source_oid FROM links
EXCEPT SELECT DISTINCT source_oid FROM links_to_root
        """)
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids

    @connect()
    def get_linked_to_oid(self, connection, oid, depth):
        cursor = connection.cursor()
        result = cursor.execute("""
WITH RECURSIVE linked_to_oid (source_oid, depth) AS (
    SELECT source_oid, 1
    FROM links
    WHERE target_oid = ?
    UNION
    SELECT link.source_oid, target.depth + 1
    FROM links AS link JOIN linked_to_oid AS target
        ON target.source_oid = link.target_oid
    WHERE target.depth < ?
    ORDER BY link.source_oid
)
SELECT DISTINCT source_oid, depth FROM linked_to_oid WHERE depth = ?
        """, (oid, depth, depth))
        linked = []
        for line in result.fetchall():
            linked.append((line[1], line[0]))
        return linked

    @connect()
    def get_path_to_oid(self, connection, from_oid, to_oid, depth):
        cursor = connection.cursor()
        result = cursor.execute("""
WITH RECURSIVE path_to_oid (source_oid, parent_oid, depth) AS (
    SELECT source_oid, ?, 1
    FROM links
    WHERE target_oid = ?
    UNION
    SELECT link.source_oid, link.target_oid, target.depth + 1
    FROM links AS link JOIN path_to_oid AS target
        ON target.source_oid = link.target_oid
    WHERE target.depth < ?
    ORDER BY link.source_oid
)
SELECT DISTINCT source_oid, parent_oid, depth FROM path_to_oid
        """, (from_oid, from_oid, depth))
        parents = {}
        for line in result.fetchall():
            info = (line[2], line[1])
            if info < parents.get(line[0], (sys.maxsize, None)):
                parents[line[0]] = info
        path = []
        parent_oid = to_oid
        while parent_oid != from_oid:
            if parent_oid not in parents:
                return None
            info = parents[parent_oid]
            path.append(info)
            parent_oid = info[1]
        path.reverse()
        path.append((len(path), to_oid))
        return path

    @connect()
    def get_missing_oids(self, connection):
        oids = set()
        cursor = connection.cursor()
        result = cursor.execute("""
SELECT a.target_oid FROM links AS a LEFT OUTER JOIN links AS b
ON a.target_oid = b.source_oid
WHERE a.target_oid > -1 AND b.source_oid IS NULL
        """)
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids

    @connect()
    def get_forward_references(self, connection, oid):
        oids = set()
        cursor = connection.cursor()
        result = cursor.execute("""
SELECT target_oid FROM links
WHERE source_oid = ? AND target_oid > -1
        """, (ZODB.utils.u64(oid), ))
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids

    @connect()
    def get_backward_references(self, connection, oid):
        oids = set([])
        cursor = connection.cursor()
        result = cursor.execute("""
SELECT source_oid FROM links
WHERE target_oid = ?
        """, (ZODB.utils.u64(oid), ))
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids
