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


class Updater(object):

    def __init__(self, dsn, name='default'):
        self.dsn = dsn
        self.name = name

    @contextlib.contextmanager
    def new_connection(self):
        connection = psycopg2.extensions.connection(self.dsn)
        cursor = connection.cursor()
        cursor.arraysize = 64
        yield cursor
        connection.commit()
        connection.close()

    def ids_batch_from_database(self, batch_size=100000):
        # Reading ids is expensive because of the sort, so we do
        # larger requests here.
        offset = 0
        count = 1
        logger.info('{}> Prepare ids'.format(self.name))
        with self.new_connection() as cursor:
            cursor.execute(
                "CREATE MATERIALIZED VIEW sorted_zoid AS "
                "SELECT zoid FROM object_state ORDER BY zoid;")

        while True:
            logger.info('{}> Fetch ids #{}'.format(self.name, count))
            with self.new_connection() as cursor:
                cursor.execute(
                    "SELECT * FROM sorted_zoid LIMIT %s OFFSET %s",
                    (batch_size, offset))
                oids = [result[0] for result in cursor.fetchall()]
            if not oids:
                break
            offset += batch_size
            count += 1
            yield oids

        logger.info('{}> Cleanup ids'.format(self.name))
        with self.new_connection() as cursor:
            cursor.execute(
                "DROP MATERIALIZED VIEW sorted_zoid")

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


def single_process(dsn, batch_size=200000):
    updater = Updater(dsn)
    ids_batch = updater.ids_batch_from_database(batch_size)
    for batch in updater.apply_batch(ids_batch):
        pass


def worker_process(incoming, outgoing, empty, dsn, name):

    def wake_up_master():
        empty.acquire()
        empty.notify()
        empty.release()

    def ids_batch_from_master():
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

    updater = Updater(dsn, name)
    try:
        for batch in updater.apply_batch(ids_batch_from_master()):
            outgoing.put(True)
    finally:
        outgoing.put(None)
        logger.info('{}> I am done'.format(name))
        wake_up_master()


def multi_process(dsn, processes_count=None, queue_size=5, batch_size=100000):
    if processes_count is None:
        processes_count = multiprocessing.cpu_count()
    workers = {}
    empty = multiprocessing.Condition()
    updater = Updater(dsn, 'master')
    fetch_factor = processes_count * queue_size * 2
    logger.info('master> Fetch factor is {}'.format(fetch_factor))

    def split_ids_batch(batch):
        if batch:
            for factor in range(fetch_factor):
                part = batch[batch_size * factor:batch_size * (factor + 1)]
                if not part:
                    break
                yield part

    batch_completed = 0
    ids_done = False
    ids_batch = updater.ids_batch_from_database(batch_size * fetch_factor)
    ids_stack = list(split_ids_batch(next(ids_batch)))

    # Create workers.
    for worker_index in range(processes_count):
        if not ids_stack:
            logger.info('master> We ran out of things to do')
            try:
                next(ids_batch)
            except StopIteration:
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
    if not ids_stack:
        # Stack is empty, we did not have enough for the first round,
        # but we should have had more than needed so it means we are
        # out of ids.
        ids_done = True

    # Feed work.
    cycle = 0
    while len(workers):
        cycle += 1
        if not ids_done and len(ids_stack) <= (fetch_factor / 2):
            # Fetch new ids to work on while workers are doing something else.
            try:
                ids = next(ids_batch)
            except StopIteration:
                logger.info('master> Got all ids #{}'.format(cycle))
                ids_done = True
            else:
                ids_stack.extend(split_ids_batch(ids))
            ids = None
        else:
            logger.info('master> Sleeping ({} batch completed) #{}'.format(
                batch_completed, cycle))
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
                if worker_result is True:
                    worker_batch_completed += 1
                if worker_result is None:
                    worker_done = True
                    break
            batch_completed += worker_batch_completed
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
    # single_process(dsn)
    multi_process(
        args.dsn,
        queue_size=args.queue_size,
        batch_size=args.batch_size)
