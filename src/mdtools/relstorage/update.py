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
        offset = 0
        while True:
            with self.new_connection() as cursor:
                cursor.execute(
                    "SELECT zoid FROM object_state "
                    "ORDER BY zoid LIMIT %s OFFSET %s",
                    (batch_size, offset))
                oids = [result[0] for result in cursor.fetchall()]
            if not oids:
                break
            yield oids
            offset += batch_size

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
                ids = incoming.get(block=True, timeout=5)
            except Queue.Empty:
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
        wake_up_master()


def multi_process(dsn, processes_count=4, queue_size=8, batch_size=100000):
    ids_done = False
    workers = {}
    empty = multiprocessing.Condition()
    updater = Updater(dsn)
    ids_batch = updater.ids_batch_from_database(batch_size)

    # Create workers.
    for worker_index in range(processes_count):
        worker_name = 'worker-{}'.format(worker_index + 1)
        incoming = multiprocessing.Queue(queue_size)
        outgoing = multiprocessing.Queue(queue_size)
        for queue_index in range(queue_size):
            try:
                ids = next(ids_batch)
            except StopIteration:
                ids_done = queue_index == 0
                break
            incoming.put(ids)
        if ids_done:
            logger.info('master> We ran out of things to do')
            break
        logger.info('master> Starting worker #{}'.format(worker_name))
        worker = multiprocessing.Process(
            target=worker_process,
            args=(incoming, outgoing, empty, dsn, worker_name))
        worker.start()
        workers[worker_name] = (worker, incoming, outgoing)

    # Feed work.
    while len(workers):
        empty.acquire()
        empty.wait()
        empty.release()

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
                logger.info('master> Worker #{} is done'.format(worker_name))
                worker.join()
                del workers[worker_name]
                continue
            if ids_completed:
                if ids_done:
                    # Send end of working.
                    incoming.put(None)
                else:
                    for queue_index in range(ids_completed):
                        try:
                            ids = next(ids_batch)
                        except StopIteration:
                            logger.info('master> We ran out of things to do')
                            ids_done = True
                            incoming.put(None)
                            break
                        incoming.put(ids)


def main():
    dsn = "dbname='maas_dev'"
    # single_process(dsn)
    multi_process(dsn)
