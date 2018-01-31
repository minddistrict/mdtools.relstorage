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
        while True:
            logger.info('{}> Fetch ids #{}'.format(self.name, count))
            with self.new_connection() as cursor:
                cursor.execute(
                    "SELECT zoid FROM object_state "
                    "ORDER BY zoid LIMIT %s OFFSET %s",
                    (batch_size, offset))
                oids = [result[0] for result in cursor.fetchall()]
            if not oids:
                break
            offset += batch_size
            count += 1
            yield oids

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
                output_file = processor.rename(decode_record(data))
                if output_file is not None:
                    result_batch.append((encode_record(output_file), oid))
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
                ids = incoming.get(block=True, timeout=1)
            except Queue.Empty:
                logger.info('{}> I am starving'.format(name))
                wake_up_master()
                continue
            if ids is None:
                break
            yield ids

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

    def split_ids_batch(batch):
        if batch:
            for factor in range(fetch_factor):
                part = batch[batch_size * factor:batch_size * (factor + 1)]
                if not part:
                    break
                yield part

    ids_done = False
    ids_batch = updater.ids_batch_from_database(batch_size * fetch_factor)
    ids_stack = list(split_ids_batch(next(ids_batch)))

    # Create workers.
    for worker_index in range(processes_count):
        if not ids_stack:
            logger.info('master> We ran out of things to do')
            break
        worker_name = 'worker-{}'.format(worker_index + 1)
        incoming = multiprocessing.Queue(queue_size + 1)
        outgoing = multiprocessing.Queue(queue_size + 1)
        for queue_index in range(queue_size):
            if not ids_stack:
                break
            incoming.put(ids_stack.pop())
        logger.info('master> Starting worker {}'.format(worker_name))
        worker = multiprocessing.Process(
            target=worker_process,
            args=(incoming, outgoing, empty, dsn, worker_name))
        worker.start()
        workers[worker_name] = (worker, incoming, outgoing)
        # Sleep to have workers out of sync
        time.sleep(5)

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
            logger.info('master> Sleeping #{}'.format(cycle))
            empty.acquire()
            empty.wait(60)
            empty.release()
            logger.info('master> Waking up #{}'.format(cycle))

        for worker_name, worker_info in list(workers.items()):
            worker, incoming, outgoing = worker_info
            ids_completed = 0
            worker_done = False
            while True:
                try:
                    worker_result = outgoing.get_nowait()
                except Queue.Empty:
                    break
                if worker_result is True:
                    ids_completed += 1
                if worker_result is None:
                    worker_done = True
                    break
            if worker_done:
                logger.info('master> Worker {} is done #{}'.format(
                    worker_name, cycle))
                worker.join()
                del workers[worker_name]
                continue
            if ids_completed:
                for queue_index in range(ids_completed):
                    if not ids_stack:
                        incoming.put(None)
                        break
                    incoming.put(ids_stack.pop())


def main():
    dsn = "dbname='maas_dev'"
    # single_process(dsn)
    multi_process(dsn)
