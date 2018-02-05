import argparse
import os
import sys
import tempfile

import ZODB.utils
import mdtools.relstorage.zodb
import mdtools.relstorage.reference


def iter_database(db):
    """Iter over records located inside the database."""
    for transaction in db.storage.iterator():
        for record in transaction:
            yield record.data, ZODB.utils.u64(record.oid)


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
        '--save-references', metavar='FILE.DB', dest='save',
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

    filename = args.save
    if not filename:
        # If we don't save, create a temporary file for the database.
        descriptor, filename = tempfile.mkstemp('zodbchecker')
    references = mdtools.relstorage.reference.Database(filename)
    if args.save:
        if args.override and os.path.isfile(args.save):
            os.unlink(args.save)
        elif references.check_database():
            parser.error('Database already initialized.')
    references.create_database()
    references.analyze_records(iter_database(db))
    print('Database created.')

    missing_oids = references.get_missing_oids()
    if not args.save:
        # Cleanup temporary file
        os.unlink(filename)

    if missing_oids:
        # We have missing objects
        print('{0} missing objects.'.format(len(missing_oids)))
        sys.exit(1)
    print('No missing objects.')
    sys.exit(0)
