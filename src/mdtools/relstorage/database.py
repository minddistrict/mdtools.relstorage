import contextlib
import logging
import multiprocessing
import psycopg2
import psycopg2.extras
import random
import time
import Queue

logger = logging.getLogger('mdtools.relstorage.database')

MAX_DELAY = 20
FETCH_MANY = 10000


# Worker logic

class Connection(object):
    _connection = None

    def __init__(self, dsn):
        self.dsn = dsn

    def new_connection(self):
        connection = psycopg2.extensions.connection(self.dsn)
        return connection

    @contextlib.contextmanager
    def new_cursor(self, *args, **kwargs):
        if self._connection is None:
            self._connection = self.new_connection()
        with self._connection.cursor(*args, **kwargs) as cursor:
            yield cursor
        self._connection.commit()


class Ids(Connection):

    def __init__(self, dsn, batch_size):
        super(Ids, self).__init__(dsn)
        assert batch_size % FETCH_MANY == 0
        self.batch_size = batch_size
        self.total = 0
        self.fetched = 0

    def fetch(self):
        logger.info('master> Batch size is {}'.format(self.batch_size))
        with self.new_cursor() as cursor:
            cursor.execute("SELECT count(*) FROM object_state;")
            self.total = cursor.fetchone()[0]
            logger.info('master> Found {} objects, estimated {} batch'.format(
                self.total, (self.total // self.batch_size) + 1))

        with self.new_cursor(name='ids') as cursor:
            cursor.execute("SELECT zoid FROM object_state ORDER BY zoid")
            while True:
                job = []

                for i in range(self.batch_size // FETCH_MANY):
                    results = cursor.fetchmany(FETCH_MANY)
                    if not results:
                        break
                    self.fetched += len(results)
                    job.extend(result[0] for result in results)

                if not job:
                    break
                yield job

        assert self.total == self.fetched


class Worker(Connection, multiprocessing.Process):
    iteration = 0

    def __init__(self, worker):
        dsn, incoming, outgoing, empty = worker
        self._empty = empty
        self._incoming = incoming
        self._outgoing = outgoing
        Connection.__init__(self, dsn)
        multiprocessing.Process.__init__(self)

    @property
    def logname(self):
        return self.name.lower()

    def new_connection(self):
        connection = super(Worker, self).new_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                "PREPARE fetch_state (int) AS "
                "SELECT encode(state, 'base64') "
                "FROM object_state WHERE zoid = $1")
            cursor.execute(
                "PREPARE update_state (text, int) AS "
                "UPDATE object_state SET state = decode($1, 'base64') "
                "WHERE zoid = $2")
        connection.commit()
        return connection

    def read_batch(self, ids):
        logger.debug('{}> Read data #{}'.format(self.logname, self.iteration))
        batch = []
        with self.new_cursor() as cursor:
            for oid in ids:
                cursor.execute("EXECUTE fetch_state (%s)", (oid,))
                result = cursor.fetchone()
                if result:
                    batch.append((result[0], oid))
                else:
                    raise AssertionError('OID disappeared')
        return batch

    def write_batch(self, batch):
        logger.debug('{}> Write data #{}'.format(self.logname, self.iteration))
        with self.new_cursor() as cursor:
            psycopg2.extras.execute_batch(
                cursor,
                "EXECUTE update_state (%s, %s)",
                batch,
                page_size=10)

    def process(self, ids):
        raise NotImplementedError()

    def _wake_up_master(self):
        self._empty.acquire()
        self._empty.notify()
        self._empty.release()

    def _ids_from_master(self):
        while True:
            try:
                order = self._incoming.get(block=True, timeout=1)
            except Queue.Empty:
                logger.debug('{}> I am starving'.format(self.logname))
                self._wake_up_master()
                continue
            if order is None:
                break
            if isinstance(order, int):
                logger.debug('{}> Waiting {} seconds'.format(
                    self.logname, order))
                time.sleep(order)
                continue
            if isinstance(order, list):
                yield order

    def run(self):
        try:
            for ids in self._ids_from_master():
                self.iteration += 1
                result = self.process(ids)
                assert isinstance(result, int)
                self._outgoing.put(result)
        finally:
            self._outgoing.put(None)
            logger.debug('{}> I am done'.format(self.logname))
            self._wake_up_master()


# End of worker logic


def multi_process(task, dsn, queue_size=5, batch_size=100000, **options):
    processes_count = multiprocessing.cpu_count()
    workers = {}
    empty = multiprocessing.Condition()
    ids = Ids(dsn, batch_size=batch_size)
    ids_consumed = False
    ids_completed = 0
    ids_fetch = ids.fetch()

    # Create workers.
    for worker_index in range(processes_count):
        if ids_consumed:
            logger.debug('master> We ran out of things to do')
            break
        incoming = multiprocessing.Queue(queue_size + 2)
        outgoing = multiprocessing.Queue(queue_size + 2)
        incoming.put(random.randint(1, MAX_DELAY))
        for queue_index in range(queue_size):
            try:
                job = next(ids_fetch)
            except StopIteration:
                ids_consumed = True
                incoming.put(None)
                break
            else:
                incoming.put(job)
        worker = task(
            worker=(dsn, incoming, outgoing, empty),
            **options)
        logger.info('master> Starting worker "{}"'.format(worker.logname))
        worker.start()
        workers[worker.logname] = (worker, incoming, outgoing)

    # Feed work.
    cycle = 0
    while len(workers):
        cycle += 1
        logger.info('master> Progress ({:.2f}% done) #{}'.format(
            (ids_completed * 100.0 / ids.total), cycle))
        empty.acquire()
        empty.wait(60)
        empty.release()
        logger.debug('master> Waking up #{}'.format(cycle))

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
                logger.info('master> Worker "{}" is done #{}'.format(
                    worker_name, cycle))
                worker.join()
                del workers[worker_name]
                continue
            if worker_batch_completed:
                if ids_consumed:
                    incoming.put(None)
                else:
                    if worker_batch_completed == queue_size:
                        incoming.put(random.randint(1, MAX_DELAY))
                    for queue_index in range(worker_batch_completed):
                        try:
                            job = next(ids_fetch)
                        except StopIteration:
                            ids_consumed = True
                            incoming.put(None)
                            break
                        else:
                            incoming.put(job)

    if ids_completed == ids.total:
        logger.info('master> All done')
    else:
        logger.error('master> Missed {} objects'.format(
            ids.total - ids_completed))
