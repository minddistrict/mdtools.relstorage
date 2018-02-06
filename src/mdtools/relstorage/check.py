import argparse
import base64
import logging
import os
import sys
import tempfile

import ZODB.utils
import ZODB.serialize
import mdtools.relstorage.database
import mdtools.relstorage.log
import mdtools.relstorage.reference
import mdtools.relstorage.zodb


logger = logging.getLogger('mdtools.relstorage.check')


def read_database(db):
    for transaction in db.storage.iterator():
        for record in transaction:
            source_oid = ZODB.utils.u64(record.oid)
            target_oids = {
                ZODB.utils.u64(reference)
                for reference in ZODB.serialize.referencesf(record.data)}
            yield source_oid, target_oids


class Reader(mdtools.relstorage.database.Worker):

    def process(self, job):
        results = []
        batch = self.read_batch(job)
        logger.debug('{}> Reading #{}'.format(
            self.logname, self.iteration))
        for data, oid in batch:
            try:
                results.append(
                    (oid,
                     {ZODB.utils.u64(reference)
                      for reference in ZODB.serialize.referencesf(
                              base64.decodestring(data))}))
            except Exception:
                logger.exception(
                    '{}> Error while reading record "0x{:x}":'.format(
                        self.logname, oid))
        self.send_to_consumer(results)
        return len(results)


class Writer(mdtools.relstorage.database.Consumer):

    def __init__(self, references, **options):
        self.references = references
        super(Writer, self).__init__(**options)

    def process(self, job):
        logging.debug('{}> Write data #{}'.format(
            self.logname, self.iteration))
        self.references.add_references(job)
        return len(job)


def zodb_main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Index relations and check for missing ones.')
    parser.add_argument(
        '--config', metavar='FILE',
        help='use a ZConfig file to specify database')
    parser.add_argument(
        '--zeo', metavar='ADDRESS',
        help='connect to ZEO server instead (host:port or socket name)')
    parser.add_argument(
        '--storage', metavar='NAME',
        help='connect to given ZEO storage')
    parser.add_argument(
        '--db', metavar='DATA.FS',
        help='use given Data.fs file')
    parser.add_argument(
        '--save-references', metavar='FILE.DB', dest='filename',
        help='save computed references in a database for reuse')
    parser.add_argument(
        '--override-references', action="store_true",
        dest="override", default=False,
        help='override a reference database')
    parser.add_argument(
        '--rw', action='store_false', dest='readonly', default=True,
        help='open the database read-write (default: read-only)')

    args = parser.parse_args(args)
    try:
        db = mdtools.relstorage.zodb.open(args)
    except ValueError as error:
        parser.error(error.args[0])

    try:
        support = db.storage.supportsUndo()
    except AttributeError:
        support = True
    if support:
        parser.error('Only supports history free databases.')

    filename = args.filename
    if not filename:
        # If we don't save, create a temporary file for the database.
        descriptor, filename = tempfile.mkstemp('zodbchecker')
    references = mdtools.relstorage.reference.Database(filename)
    if args.filename:
        if args.override and os.path.isfile(args.filename):
            os.unlink(args.filename)
        elif references.check_database():
            parser.error('Database already initialized.')
    references.create_database()
    references.add_references(read_database(db))
    references.finish_database()

    print('Database created')
    missing_oids = references.get_missing_oids()
    if not args.filename:
        # Cleanup temporary file
        os.unlink(filename)

    if missing_oids:
        # We have missing objects
        print('{0} missing objects'.format(len(missing_oids)))
        sys.exit(1)
    print('No missing objects')
    sys.exit(0)


def relstorage_main():
    parser = argparse.ArgumentParser(description="ZODB search on relstorage")
    parser.add_argument(
        '--queue-size', dest='queue_size', type=int, default=5)
    parser.add_argument(
        '--batch-size', dest='batch_size', type=int, default=100000)
    parser.add_argument(
        '--save-references', metavar='FILE.DB', dest='filename',
        help='save computed references in a database for reuse')
    parser.add_argument(
        '--override-references', action="store_true",
        dest="override", default=False,
        help='override a reference database')
    parser.add_argument(
        "--quiet", action="store_true", help="suppress non-error messages")
    parser.add_argument(
        "--verbose", action="store_true", help="more verbose output")
    parser.add_argument(
        'dsn', help="DSN example: dbname='maas_dev'")

    args = parser.parse_args()
    mdtools.relstorage.log.setup(args)

    filename = args.filename
    if not filename:
        # If we don't save, create a temporary file for the database.
        descriptor, filename = tempfile.mkstemp('zodbchecker')
    references = mdtools.relstorage.reference.Database(filename)
    if args.filename:
        if args.override and os.path.isfile(args.filename):
            os.unlink(args.filename)
        elif references.check_database():
            parser.error('Database already initialized.')
    references.create_database()

    mdtools.relstorage.database.multi_process(
        dsn=args.dsn,
        worker_task=Reader,
        consumer_task=Writer,
        consumer_options={
            'references': references},
        queue_size=args.queue_size,
        batch_size=args.batch_size)

    references.finish_database()
    print('Looking for missing objects')
    missing_oids = references.get_missing_oids()
    if not args.filename:
        # Cleanup temporary file
        os.unlink(filename)

    if missing_oids:
        # We have missing objects
        print('{0} missing objects'.format(len(missing_oids)))
        sys.exit(1)
    print('No missing objects')
    sys.exit(0)
