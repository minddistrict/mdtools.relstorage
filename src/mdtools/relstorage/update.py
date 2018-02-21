import argparse
import io
import logging
import sys
import mdtools.relstorage.database
import mdtools.relstorage.log
import zodbupdate.convert
import zodbupdate.serialize

logger = logging.getLogger('mdtools.relstorage.update')


# Updater logic

def create_processor():
    return zodbupdate.serialize.ObjectRenamer(
        renames=zodbupdate.convert.default_renames().copy(),
        decoders=zodbupdate.convert.load_decoders().copy(),
        pickle_protocol=3,
        repickle_all=True)


class Updater(mdtools.relstorage.database.Worker):

    def __init__(self, **options):
        self.processor = create_processor()
        super(Updater, self).__init__(**options)

    def process(self, ids):
        result = []
        batch = self.read_batch(ids)
        logger.debug('{}> Processing data #{}'.format(
            self.logname, self.iteration))
        for data, oid in batch:
            try:
                output_file = self.processor.rename(io.BytesIO(data))
                if output_file is not None:
                    result.append((output_file.getvalue(), oid))
            except Exception:
                logger.exception(
                    '{}> Error while processing record "0x{:x}":'.format(
                        self.logname, oid))
        self.write_batch(result)
        return len(result)


# End of updater logic


def relstorage_main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description="ZODB update on relstorage")
    parser.add_argument(
        '--queue-size', dest='queue_size', type=int, default=4)
    parser.add_argument(
        '--batch-size', dest='batch_size', type=int, default=100000)
    parser.add_argument(
        "--quiet", action="store_true", help="suppress non-error messages")
    parser.add_argument(
        "--verbose", action="store_true", help="more verbose output")
    parser.add_argument(
        'dsn',
        help="DSN example: dbname='maas_dev'")

    args = parser.parse_args(args)
    mdtools.relstorage.log.setup(args)
    mdtools.relstorage.database.multi_process(
        dsn=args.dsn,
        worker_task=Updater,
        queue_size=args.queue_size,
        batch_size=args.batch_size)
