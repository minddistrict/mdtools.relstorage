import contextlib
import logging
import multiprocessing
import psycopg2
import psycopg2.extras
import Queue

logger = logging.getLogger('mdtools.relstorage.database')

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
        dsn, incoming, outgoing, control, lock, condition = worker
        self._condition = condition
        self._incoming = incoming
        self._outgoing = outgoing
        self._control = control
        self._lock = lock
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

    def send_to_consumer(self, batch):
        assert self._lock is not None and self._outgoing is not None
        self._lock.acquire()
        self._outgoing.put(batch)
        self._lock.release()

    def process(self, job):
        raise NotImplementedError()

    def _wake_up_master(self):
        self._condition.acquire()
        self._condition.notify()
        self._condition.release()

    def _job_from_master(self):
        while True:
            try:
                order = self._incoming.get(block=True, timeout=1)
            except Queue.Empty:
                logger.debug('{}> I am starving'.format(self.logname))
                self._wake_up_master()
                continue
            if order is None:
                break
            if isinstance(order, list):
                yield order

    def run(self):
        try:
            for job in self._job_from_master():
                self.iteration += 1
                result = self.process(job)
                assert isinstance(result, int)
                self._control.put(result)
        finally:
            self._control.put(None)
            logger.debug('{}> I am done'.format(self.logname))
            self._wake_up_master()


class Consumer(multiprocessing.Process):
    iteration = 0

    def __init__(self, worker):
        outgoing, control, condition = worker
        self._outgoing = outgoing
        self._condition = condition
        self._control = control
        multiprocessing.Process.__init__(self)

    @property
    def logname(self):
        return self.name.lower()

    def process(self, job):
        raise NotImplementedError()

    def _wake_up_master(self):
        self._condition.acquire()
        self._condition.notify()
        self._condition.release()

    def _job_from_workers(self):
        while True:
            try:
                order = self._outgoing.get(block=True)
            except Queue.Empty:
                continue
            if order is None:
                break
            if isinstance(order, list):
                yield order

    def run(self):
        try:
            for job in self._job_from_workers():
                self.iteration += 1
                result = self.process(job)
                assert isinstance(result, int)
                self._control.put(result)
        finally:
            self._control.put(None)
            logger.debug('{}> I am done'.format(self.logname))
            self._wake_up_master()


# End of worker logic


def multi_process(
        dsn,
        worker_task,
        worker_options=None,
        consumer_task=None,
        consumer_options=None,
        queue_size=5,
        batch_size=100000):
    ids = Ids(dsn, batch_size=batch_size)
    ids_consumed = False
    ids_fetch = ids.fetch()

    processes_count = multiprocessing.cpu_count()
    workers = {}
    worker_ids_completed = 0
    consumer_ids_completed = 0
    master_condition = multiprocessing.Condition()
    if consumer_task is not None:
        outgoing_lock = multiprocessing.Lock()
        outgoing = multiprocessing.Queue(queue_size + 2)
        consumer_control = multiprocessing.Queue(
            queue_size * processes_count + 2)
        consumer = consumer_task(
            worker=(outgoing, consumer_control, master_condition),
            **(consumer_options or {}))
        logger.info('master> Starting consumer "{}"'.format(consumer.logname))
        consumer.start()
    else:
        outgoing_lock = None
        outgoing = None
        consumer_control = None
        consumer = None

    # Create workers.
    for worker_index in range(processes_count):
        if ids_consumed:
            logger.debug('master> We ran out of things to do')
            break
        incoming = multiprocessing.Queue(queue_size + 2)
        control = multiprocessing.Queue(queue_size + 2)
        for queue_index in range(queue_size):
            try:
                job = next(ids_fetch)
            except StopIteration:
                ids_consumed = True
                incoming.put(None)
                break
            else:
                incoming.put(job)
        worker = worker_task(
            worker=(
                dsn, incoming, outgoing, control,
                outgoing_lock, master_condition),
            **(worker_options or {}))
        logger.info('master> Starting worker "{}"'.format(worker.logname))
        worker.start()
        workers[worker.logname] = (worker, incoming, control)

    # Feed work.
    cycle = 0
    cycle_activity = True
    while len(workers) or consumer is not None:
        cycle += 1
        if not cycle_activity:
            logger.info(
                'master> Progress '
                '({:.2f}% worked, {:.2f}% consumed) #{}'.format(
                    (worker_ids_completed * 100.0 / ids.total),
                    (consumer_ids_completed * 100.0 / ids.total),
                    cycle))
            master_condition.acquire()
            master_condition.wait(30)
            master_condition.release()
            logger.debug('master> Waking up #{}'.format(cycle))
            cycle_activity = False

        for worker_name, worker_info in list(workers.items()):
            worker, incoming, control = worker_info
            worker_batch_completed = 0
            worker_done = False
            while True:
                try:
                    worker_status = control.get_nowait()
                except Queue.Empty:
                    break
                if worker_status is None:
                    worker_done = True
                    break
                worker_batch_completed += 1
                worker_ids_completed += worker_status
            if worker_done:
                cycle_activity = True
                logger.info('master> Worker "{}" is done #{}'.format(
                    worker_name, cycle))
                worker.join()
                del workers[worker_name]
                continue
            if worker_batch_completed:
                cycle_activity = True
                if ids_consumed:
                    incoming.put(None)
                else:
                    for queue_index in range(worker_batch_completed):
                        try:
                            job = next(ids_fetch)
                        except StopIteration:
                            ids_consumed = True
                            incoming.put(None)
                            break
                        else:
                            incoming.put(job)

        if consumer is not None:
            if not len(workers):
                outgoing.put(None)
            while True:
                try:
                    consumer_status = consumer_control.get_nowait()
                except Queue.Empty:
                    break
                if consumer_status is None:
                    logger.info('master> Consumer is done #{}'.format(cycle))
                    consumer.join()
                    consumer = None
                    break
                consumer_ids_completed += consumer_status

    if worker_ids_completed == ids.total:
        logger.info('master> All done')
    else:
        logger.error('master> Missed {} objects'.format(
            ids.total - worker_ids_completed))
