import argparse
import sys

import ZODB.utils
import mdtools.relstorage.zodb
import mdtools.relstorage.reference


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Find which records are links together.')
    parser.add_argument(
        '--references', metavar='FILE.DB', dest='refsdb',
        help='reference information computed by zodbcheck')
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
        '--depth', metavar='DEPTH', dest='depth', type='int',
        help='depth to explore', default=1)
    parser.add_argument(
        '--rw', action='store_false', dest='readonly', default=True,
        help='open the database read-write (default: read-only)')
    parser.add_argument(
        '--oid', metavar='OID', dest='oid', type='int',
        help='oid')
    args = parser.parse_args(args)
    try:
        references = mdtools.relstorage.reference.Database(args.refsdb)
    except ValueError as error:
        parser.error(error.args[0])
    try:
        db = mdtools.relstorage.zodb.open(args)
    except ValueError as error:
        parser.error(error.args[0])
    connection = db.open()
    results = references.get_linked_to_oid(args.oid, args.depth)
    print('Found {} results'.format(len(results)))
    for oid, depth in results:
        print('{}: 0x{:x}: {}'.format(
            depth,
            oid,
            connection.get(ZODB.utils.p64(oid))))
