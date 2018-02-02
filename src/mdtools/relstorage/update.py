import argparse
import random
import time
import io
import contextlib
import base64
import psycopg2
import multiprocessing
import Queue
import zodbupdate.convert
import zodbupdate.serialize
import zodbupdate.main

logger = zodbupdate.main.setup_logger()

MAX_DELAY = 20


# Updater logic

def create_processor():
    return zodbupdate.serialize.ObjectRenamer(
        renames=zodbupdate.convert.default_renames().copy(),
        decoders=zodbupdate.convert.load_decoders().copy(),
        pickle_protocol=3,
        repickle_all=True)


def decode_record(data):
    return io.BytesIO(base64.decodestring(data))


def encode_record(output_file):
    return base64.encodestring(output_file.getvalue())


class Connection(object):

    def __init__(self, dsn, name):
        self.dsn = dsn
        self.name = name

    @contextlib.contextmanager
    def new_connection(self):
        connection = psycopg2.extensions.connection(self.dsn)
        cursor = connection.cursor()
        yield cursor
        connection.commit()
        connection.close()


class Ids(Connection):

    def __init__(self, dsn, batch_size, fetch_factor):
        super(Ids, self).__init__(dsn, 'master')
        self.fetch_factor = fetch_factor
        self.batch_size = batch_size
        self.total = 0
        self.fetched = 0

    def split(self, batch):
        if batch:
            len_batch = len(batch)
            len_parts = 0
            for f in range(self.fetch_factor):
                part = batch[self.batch_size * f:self.batch_size * (f + 1)]
                len_parts += len(part)
                if not part:
                    break
                yield part
            assert len_parts == len_batch

    def fetch(self):
        # Reading ids is expensive because of the sort, so we do
        # larger requests here.
        fetch_offset = 0
        fetch_count = 1
        fetch_size = self.batch_size * self.fetch_factor
        logger.info('{}> Fetch factor is {}, batch size is {}'.format(
            self.name, self.fetch_factor, self.batch_size))
        with self.new_connection() as cursor:
            cursor.execute(
                "DROP MATERIALIZED VIEW IF EXISTS sorted_zoid")
            cursor.execute(
                "CREATE MATERIALIZED VIEW sorted_zoid AS "
                "SELECT zoid FROM object_state ORDER BY zoid;")
            self.total = int(cursor.statusmessage.split(' ')[1])
            logger.info('{}> Found {} objects, estimated {} batch'.format(
                self.name, self.total, (self.total // self.batch_size) + 1))

        while True:
            logger.info('{}> Fetch ids #{}'.format(self.name, fetch_count))
            with self.new_connection() as cursor:
                cursor.execute(
                    "SELECT * FROM sorted_zoid LIMIT %s OFFSET %s",
                    (fetch_size, fetch_offset))
                ids = []
                results = cursor.fetchmany(5000)
                while results:
                    ids.extend(result[0] for result in results)
                    results = cursor.fetchmany(5000)
            self.fetched += len(ids)
            if not ids:
                break
            fetch_offset += fetch_size
            fetch_count += 1
            yield list(self.split(ids))

        assert self.total == self.fetched
        logger.info('{}> Cleanup ids #{}'.format(self.name, fetch_count))
        with self.new_connection() as cursor:
            cursor.execute(
                "DROP MATERIALIZED VIEW sorted_zoid")


class Worker(Connection):

    def read_batch(self, ids_batch):
        for index, oids in enumerate(ids_batch):
            logger.info('{}> Read data #{}'.format(self.name, index + 1))
            batch = []
            with self.new_connection() as cursor:
                for oid in oids:
                    cursor.execute(
                        "SELECT encode(state, 'base64') "
                        "FROM object_state WHERE zoid = %s",
                        (oid,))
                    result = cursor.fetchone()
                    if result:
                        batch.append((result[0], oid))
                    else:
                        raise AssertionError('OID disappeared')
            yield batch

    def write_batch(self, processor):
        for index, batch in enumerate(processor):
            logger.info('{}> Write data #{}'.format(self.name, index + 1))
            with self.new_connection() as cursor:
                cursor.executemany(
                    "UPDATE object_state SET state = decode(%s, 'base64') "
                    "WHERE zoid = %s",
                    batch)
            yield batch

    def process_batch(self, read_batch):
        processor = create_processor()
        for index, incoming_batch in enumerate(read_batch):
            logger.info('{}> Processing #{}'.format(self.name, index+1))
            result_batch = []
            for data, oid in incoming_batch:
                try:
                    output_file = processor.rename(decode_record(data))
                    if output_file is not None:
                        result_batch.append((encode_record(output_file), oid))
                except Exception:
                    logger.exception(
                        '{}> Error while processing record'.format(self.name))
            yield result_batch

    def apply_batch(self, ids):
        return self.write_batch(self.process_batch(self.read_batch(ids)))


# End of updater logic


def worker_process(incoming, outgoing, empty, dsn, name):

    def wake_up_master():
        empty.acquire()
        empty.notify()
        empty.release()

    def ids_from_master():
        while True:
            try:
                order = incoming.get(block=True, timeout=1)
            except Queue.Empty:
                logger.info('{}> I am starving'.format(name))
                wake_up_master()
                continue
            if order is None:
                break
            if isinstance(order, int):
                logger.info('{}> Waiting {} seconds'.format(name, order))
                time.sleep(order)
                continue
            if isinstance(order, list):
                yield order

    worker = Worker(dsn, name)
    try:
        for batch in worker.apply_batch(ids_from_master()):
            outgoing.put(len(batch))
    finally:
        outgoing.put(None)
        logger.info('{}> I am done'.format(name))
        wake_up_master()


def multi_process(dsn, queue_size=5, batch_size=100000):
    processes_count = multiprocessing.cpu_count()
    workers = {}
    empty = multiprocessing.Condition()
    fetch_factor = processes_count * queue_size * 2
    ids = Ids(dsn, batch_size=batch_size, fetch_factor=fetch_factor)
    ids_consumed = False
    ids_fetch = ids.fetch()
    ids_stack = next(ids_fetch)
    ids_completed = 0

    # Create workers.
    for worker_index in range(processes_count):
        if not ids_stack:
            logger.info('master> We ran out of things to do')
            try:
                next(ids_fetch)
            except StopIteration:
                ids_consumed = True
                break
            else:
                raise AssertionError('Should not happen')
        worker_name = 'worker-{}'.format(worker_index + 1)
        incoming = multiprocessing.Queue(queue_size + 2)
        outgoing = multiprocessing.Queue(queue_size + 2)
        incoming.put(random.randint(1, MAX_DELAY))
        for queue_index in range(queue_size):
            if not ids_stack:
                incoming.put(None)
                break
            incoming.put(ids_stack.pop())
        logger.info('master> Starting worker {}'.format(worker_name))
        worker = multiprocessing.Process(
            target=worker_process,
            args=(incoming, outgoing, empty, dsn, worker_name))
        worker.start()
        workers[worker_name] = (worker, incoming, outgoing)

    # Feed work.
    cycle = 0
    while len(workers):
        cycle += 1
        if not ids_consumed and len(ids_stack) <= (fetch_factor / 2):
            # Fetch new ids to work on while workers are doing something else.
            try:
                ids_stack.extend(next(ids_fetch))
            except StopIteration:
                logger.info('master> Got all ids #{}'.format(cycle))
                ids_consumed = True
        else:
            logger.info('master> Sleeping ({}%) #{}'.format(
                (ids_completed * 100 / ids.total), cycle))
            empty.acquire()
            empty.wait(60)
            empty.release()
            logger.info('master> Waking up #{}'.format(cycle))

        for worker_name, worker_info in list(workers.items()):
            worker, incoming, outgoing = worker_info
            worker_batch_completed = 0
            worker_done = False
            while True:
                try:
                    worker_result = outgoing.get_nowait()
                except Queue.Empty:
                    break
                if worker_result is None:
                    worker_done = True
                    break
                worker_batch_completed += 1
                ids_completed += worker_result
            if worker_done:
                logger.info('master> Worker {} is done #{}'.format(
                    worker_name, cycle))
                worker.join()
                del workers[worker_name]
                continue
            if worker_batch_completed:
                if worker_batch_completed == queue_size:
                    incoming.put(random.randint(1, MAX_DELAY))
                for queue_index in range(worker_batch_completed):
                    if not ids_stack:
                        incoming.put(None)
                        break
                    incoming.put(ids_stack.pop())

    if ids_completed == ids.total:
        logger.info('master> All done')
    else:
        logger.error('master> Missed {} objects'.format(
            ids_completed - ids.total))


def main():
    parser = argparse.ArgumentParser(description="ZODB update on relstorage")
    parser.add_argument(
        '--queue-size', dest='queue_size', type=int, default=5)
    parser.add_argument(
        '--batch-size', dest='batch_size', type=int, default=100000)
    parser.add_argument(
        'dsn',
        help="DSN example: dbname='maas_dev'")

    args = parser.parse_args()
    multi_process(
        args.dsn,
        queue_size=args.queue_size,
        batch_size=args.batch_size)
