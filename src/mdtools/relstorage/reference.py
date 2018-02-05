import logging
import sqlite3

import ZODB.utils
import ZODB.serialize
import zope.interface
import mdtools.relstorage.interfaces

COMMIT_EVERY = 50000

logger = logging.getLogger('mdtools.relstorage.reference')


def connect(callback):
    """Decorator for the reference database to access the sqlite DB."""

    def wrapper(self, *args, **kwargs):
        try:
            connection = sqlite3.connect(self.db_name)
        except Exception:
            raise ValueError(
                'impossible to open references database {}.'.format(
                    self.db_name))
        try:
            result = callback(self, connection, *args, **kwargs)
        finally:
            connection.close()
        return result

    return wrapper


@zope.interface.implementer(mdtools.relstorage.interfaces.IDatabase)
class Database(object):

    def __init__(self, db_name):
        self.db_name = db_name

    @connect
    def analyze_records(self, connection, records):
        counter = 0
        cursor = connection.cursor()
        for data, current_oid in records:
            referred_oids = {
                ZODB.utils.u64(reference)
                for reference in ZODB.serialize.referencesf(data)}

            for referred_oid in referred_oids or [-1]:
                counter += 1
                cursor.execute("""
INSERT INTO links (source_oid, target_oid) VALUES
(?, ?)
            """, (current_oid, referred_oid))
            if counter > COMMIT_EVERY:
                connection.commit()
                counter = 0
        if counter:
            connection.commit()
        logger.info('Optimizing reference database.')
        cursor.execute("""
PRAGMA optimize;
""")

    @connect
    def create_database(self, connection):
        cursor = connection.cursor()
        cursor.execute("""
PRAGMA main.locking_mode=EXCLUSIVE;
""")
        cursor.execute("""
PRAGMA journal_mode=WAL;
""")
        cursor.execute("""
CREATE TABLE IF NOT EXISTS links
(source_oid BIGINT, target_oid BIGINT)
        """)
        cursor.execute("""
CREATE INDEX IF NOT EXISTS source_oid_index ON links (source_oid)
        """)
        cursor.execute("""
CREATE INDEX IF NOT EXISTS target_oid_index ON links (target_oid)
        """)
        connection.commit()

    @connect
    def check_database(self, connection):
        cursor = connection.cursor()
        try:
            result = cursor.execute("SELECT count(*) FROM links")
            result.fetchall()
        except sqlite3.OperationalError:
            return False
        return True

    @connect
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

    @connect
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
            linked.append((line[0], line[1]))
        return linked

    @connect
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

    @connect
    def get_forward_references(self, connection, oid):
        oids = set()
        cursor = connection.cursor()
        result = cursor.execute("""
SELECT target_oid FROM links
WHERE source_oid = ? AND target_oid > -1
        """, (u64(oid), ))
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids

    @connect
    def get_backward_references(self, connection, oid):
        oids = set([])
        cursor = connection.cursor()
        result = cursor.execute("""
SELECT source_oid FROM links
WHERE target_oid = ?
        """, (u64(oid), ))
        for oid in result.fetchall():
            oids.add(oid[0])
        return oids
