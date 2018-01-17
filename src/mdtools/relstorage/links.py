import logging
import optparse
import sys

from ZODB.utils import p64
from mdtools.relstorage.util import open_database
from mdtools.relstorage.references import ReferencesDatabase


def main(args=None):
    logging.basicConfig(format="%(message)s")

    if args is None:
        args = sys.argv[1:]

    parser = optparse.OptionParser(
        'usage: %prog [options]',
        prog='zodblinks',
        description='Find which records are links together.')
    parser.add_option('--references', metavar='FILE.DB', dest='refsdb',
                      help='reference information computed by zodbcheck')
    parser.add_option('--config', metavar='FILE',
                      help='use a ZConfig file to specify database')
    parser.add_option('--zeo', metavar='ADDRESS',
                      help='connect to ZEO server instead'
                      ' (host:port or socket name)')
    parser.add_option('--storage', metavar='NAME',
                      help='connect to given ZEO storage')
    parser.add_option('--db', metavar='DATA.FS',
                      help='use given Data.fs file')
    parser.add_option('--depth', metavar='DEPTH', dest='depth', type='int',
                      help='depth to explore', default=1)
    parser.add_option('--rw', action='store_false', dest='readonly',
                      default=True,
                      help='open the database read-write (default: read-only)')
    parser.add_option('--oid', metavar='OID', dest='oid', type='int',
                      help='oid')
    opts, args = parser.parse_args(args)
    try:
        refs = ReferencesDatabase(opts.refsdb)
    except ValueError as error:
        parser.error(error.args[0])
    try:
        db = open_database(opts)
    except ValueError as error:
        parser.error(error.args[0])
    connection = db.open()
    results = refs.linkedToOID(opts.oid, opts.depth)
    print 'Found {} results'.format(len(results))
    for oid, depth in results:
        print '{}: 0x{:x}: {}'.format(depth, oid, connection.get(p64(oid)));
