import argparse
import io
import logging
import sys

import ZODB.broken
import zodbupdate.serialize
import zodbupdate.utils
import mdtools.relstorage.log
import mdtools.relstorage.zodb
import mdtools.relstorage.database


logger = logging.getLogger('mdtools.relstorage.search')


class Search(object):

    def __init__(self, classes=[], search_data=False):
        self.classes = classes
        self.search_data = search_data

    def _validate(self, cls_info, cls):
        fullname = '.'.join(cls_info)
        if zodbupdate.utils.is_broken(cls):
            logger.error(
                'Broken class {} in record "0x{:x}"'.format(
                    fullname, self._current_oid))
        if self.classes and fullname in self.classes:
            logger.info(
                'Instance of {} found  in record "0x{:x}"'.format(
                    fullname, self._current_oid))

    def _read_class_meta(self, class_meta):
        if isinstance(class_meta, tuple):
            symb, _ = class_meta
            if isinstance(symb, tuple):
                self._validate(symb, ZODB.broken.find_global(
                    *symb, Broken=zodbupdate.serialize.ZODBBroken))
            else:
                symb_info = (getattr(symb, '__module__', None),
                             getattr(symb, '__name__', None))
                self._validate(symb_info, symb)

    def _find_global(self, *cls_info):
        cls = ZODB.broken.find_global(
            *cls_info,
            Broken=zodbupdate.serialize.ZODBBroken)
        self._validate(cls_info, cls)
        return cls

    def _persistent_load(self, reference):
        cls_info = None
        if isinstance(reference, tuple):
            oid, cls_info = reference
        if isinstance(reference, list):
            mode, information = reference
            if mode == 'm':
                db, oid, cls_info = information
        if isinstance(cls_info, tuple):
            self._find_global(cls_info)

    def search(self, data, oid):
        self._current_oid = oid
        unpickler = zodbupdate.utils.Unpickler(
            io.BytesIO(data),
            self._persistent_load,
            self._find_global)
        self._read_class_meta(unpickler.load())
        if self.search_data:
            unpickler.load()


class Searcher(Search, mdtools.relstorage.database.Worker):

    def __init__(self, worker=None, **kwargs):
        Search.__init__(self, **kwargs)
        mdtools.relstorage.database.Worker.__init__(self, worker=worker)

    def process(self, ids):
        done = 0
        batch = self.read_batch(ids)
        logger.debug('{}> Searching #{}'.format(
            self.logname, self.iteration))
        for data, oid in batch:
            try:
                self.search(data, oid)
            except Exception:
                logger.exception(
                    '{}> Error while searching record "0x{:x}":'.format(
                        self.logname, oid))
            else:
                done += 1
        return done, len(batch)


def zodb_main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Search if Python classes are used in a database.')
    parser.add_argument(
        '--config', metavar='FILE',
        help='use a ZConfig file to specify database')
    parser.add_argument(
        '--zeo', metavar='ADDRESS',
        help='connect to ZEO server instead (host:port or socket name)')
    parser.add_argument(
        '--storage', metavar='NAME', help='connect to given ZEO storage')
    parser.add_argument(
        '--db', metavar='DATA.FS', help='use given Data.fs file')
    parser.add_argument(
        '--data', action="store_true", dest="search_data", default=False,
        help='check inside persisted data too')
    parser.add_argument(
        "--quiet", action="store_true", help="suppress non-error messages")
    parser.add_argument(
        "--verbose", action="store_true", help="more verbose output")
    parser.add_argument('classes', metavar='classes', nargs='*')
    args = parser.parse_args(args)

    mdtools.relstorage.log.setup(args)
    try:
        db = mdtools.relstorage.zodb.open(args)
    except ValueError as error:
        parser.error(error.args[0])

    search = Search(
        classes=args.classes,
        search_data=args.search_data)
    for transaction in db._storage.iterator():
        for record in transaction:
            search.search(record.data, ZODB.utils.u64(record.oid))


def relstorage_main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description="ZODB search on relstorage")
    parser.add_argument(
        '--queue-size', dest='queue_size', type=int, default=4)
    parser.add_argument(
        '--batch-size', dest='batch_size', type=int, default=100000)
    parser.add_argument(
        '--data', action="store_true", dest="search_data", default=False,
        help='check inside persisted data too')
    parser.add_argument(
        "--quiet", action="store_true", help="suppress non-error messages")
    parser.add_argument(
        "--verbose", action="store_true", help="more verbose output")
    parser.add_argument(
        'dsn', help="DSN example: dbname='maas_dev'")
    parser.add_argument('classes', metavar='classes', nargs='*')

    args = parser.parse_args(args)
    mdtools.relstorage.log.setup(args)
    mdtools.relstorage.database.multi_process(
        dsn=args.dsn,
        worker_task=Searcher,
        worker_options={
            'classes': args.classes,
            'search_data': args.search_data},
        queue_size=args.queue_size,
        batch_size=args.batch_size)
