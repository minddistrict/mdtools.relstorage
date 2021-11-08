import argparse
import sys

import ZODB.utils
import mdtools.relstorage.zodb
import mdtools.relstorage.reference


def oid(oid):
    return int(oid, 16)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Find which records are links together.')
    parser.add_argument(
        '--references',
        metavar='FILE.DB',
        dest='refsdb',
        help='reference information computed by zodbcheck')
    parser.add_argument(
        '--config',
        metavar='FILE',
        help='use a ZConfig file to specify database')
    parser.add_argument(
        '--zeo',
        metavar='ADDRESS',
        help='connect to ZEO server instead (host:port or socket name)')
    parser.add_argument(
        '--storage', metavar='NAME', help='connect to given ZEO storage')
    parser.add_argument(
        '--db', metavar='DATA.FS', help='use given Data.fs file')
    parser.add_argument(
        '--depth',
        metavar='DEPTH',
        dest='depth',
        type=int,
        help='depth to explore',
        default=1)
    parser.add_argument(
        '--rw',
        action='store_false',
        dest='readonly',
        default=True,
        help='open the database read-write (default: read-only)')
    parser.add_argument(
        '--from-oid',
        metavar='FROM OID',
        dest='from_oid',
        type=oid,
        help='search from the given oid')
    parser.add_argument(
        '--to-oid',
        metavar='TO OID',
        dest='to_oid',
        type=oid,
        help='search to the given oid',
        default=0)
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
    if args.to_oid:
        results = references.get_path_to_oid(
            args.from_oid, args.to_oid, args.depth)
        if results is None:
            print(
                'Could not find a path from 0x{:x} to 0x{:x}'.format(
                    args.from_oid, args.to_oid))
            return
        else:
            print(
                'Path from 0x{:x} to 0x{:x} contains {} items:'.format(
                    args.from_oid, args.to_oid, len(results)))
    else:
        results = references.get_linked_to_oid(args.from_oid, args.depth)
        print(
            'Found {} items linked to 0x{:x} at depth {}:'.format(
                len(results), args.from_oid, args.depth))
    for depth, found_id in results:
        print(
            '{}: 0x{:x}: {}'.format(
                depth, found_id, connection.get(ZODB.utils.p64(found_id))))
