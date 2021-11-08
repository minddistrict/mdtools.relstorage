import ZODB.utils
import contextlib
import logging
import time
import queue
import multiprocessing
import dateutil.parser
import persistent.TimeStamp
import psycopg2
import psycopg2.extras

logger = logging.getLogger('mdtools.relstorage.database')

FETCH_MANY = 10000


def date_to_tid(date_as_str):
    date = dateutil.parser.parse(date_as_str)
    stamp = persistent.TimeStamp.TimeStamp(
        date.year, date.month, date.day, date.hour, date.minute, date.second)
    return ZODB.utils.u64(stamp.raw())


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

    def __init__(self, dsn, batch_size, min_date=None, max_date=None):
        super(Ids, self).__init__(dsn)
        assert batch_size % FETCH_MANY == 0
        self.min_date = min_date
        self.max_date = max_date
        self.batch_size = batch_size
        self.total = 0
        self.fetched = 0

    def fetch(self):
        logger.info('master> Batch size is {}'.format(self.batch_size))

        where = []
        params = ()
        if self.min_date is not None:
            where.append('tid > %s')
            params += (date_to_tid(self.min_date), )
        if self.max_date is not None:
            where.append('tid < %s')
            params += (date_to_tid(self.max_date), )
        if where:
            filters = ' WHERE {}'.format(' AND '.join(where))
            sorted_filters = filters + ' ORDER BY tid, zoid'
        else:
            filters = ''
            sorted_filters = ' ORDER BY zoid'

        with self.new_cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM object_state" + filters, params)
            self.total = cursor.fetchone()[0]
            logger.info(
                'master> Found {} objects, estimated {} batch'.format(
                    self.total, (self.total // self.batch_size) + 1))

        with self.new_cursor(name='ids') as cursor:
            cursor.execute(
                "SELECT zoid FROM object_state" + sorted_filters, params)
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


class Progress:

    def __init__(self, ids):
        self.start = time.time()
        self.ids = ids

    def _eta(self, completed):
        if not completed:
            return 'n/a'
        eta = self.start + ((self.ids.total / float(completed)) *
                            (time.time() - self.start))
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(eta))

    def _percent(self, completed):
        return completed * 100.0 / self.ids.total

    def report(self, cycle, worker_completed, consumer_completed):
        if consumer_completed:
            logger.info(
                'master> Progress '
                '({:.2f}% processed, {:.2f}% consumed, eta {}) #{}'.format(
                    self._percent(worker_completed),
                    self._percent(consumer_completed),
                    self._eta(consumer_completed), cycle))
        else:
            logger.info(
                'master> Progress ({:.2f}% done, eta {}) #{}'.format(
                    self._percent(worker_completed),
                    self._eta(worker_completed), cycle))


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
                "SELECT state FROM object_state WHERE zoid = $1")
            cursor.execute(
                "PREPARE update_state (bytea, int) AS "
                "UPDATE object_state SET state = $1 WHERE zoid = $2")
        connection.commit()
        return connection

    def read_batch(self, ids):
        logger.debug('{}> Read data #{}'.format(self.logname, self.iteration))
        batch = []
        with self.new_cursor() as cursor:
            for oid in ids:
                cursor.execute("EXECUTE fetch_state (%s)", (oid, ))
                result = cursor.fetchone()
                if result:
                    batch.append((bytes(result[0]), oid))
                else:
                    raise AssertionError('OID disappeared')
        return batch

    def write_batch(self, batch):
        logger.debug(
            '{}> Write data #{}'.format(self.logname, self.iteration))
        with self.new_cursor() as cursor:
            psycopg2.extras.execute_batch(
                cursor,
                "EXECUTE update_state (%s, %s)",
                ((psycopg2.Binary(data), oid) for data, oid in batch),
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
            except queue.Empty:
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
                changed, total = self.process(job)
                assert isinstance(changed, int)
                assert isinstance(total, int)
                self._control.put((changed, total))
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
            except queue.Empty:
                continue
            if order is None:
                break
            if isinstance(order, list):
                yield order

    def run(self):
        try:
            for job in self._job_from_workers():
                self.iteration += 1
                changed, total = self.process(job)
                assert isinstance(changed, int)
                assert isinstance(total, int)
                self._control.put((changed, total))
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
        queue_size=4,
        batch_size=100000,
        min_date=None,
        max_date=None):
    ids = Ids(dsn, batch_size, min_date, max_date)
    ids_consumed = False
    ids_fetch = ids.fetch()

    progress = Progress(ids)

    processes_count = multiprocessing.cpu_count()
    workers = {}
    worker_ids_changed = 0
    worker_ids_completed = 0
    consumer_ids_changed = 0
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
                dsn, incoming, outgoing, control, outgoing_lock,
                master_condition),
            **(worker_options or {}))
        logger.info('master> Starting worker "{}"'.format(worker.logname))
        worker.start()
        workers[worker.logname] = (worker, incoming, control)

    # Feed work.
    cycle = 0
    while len(workers) or consumer is not None:
        cycle += 1
        progress.report(cycle, worker_ids_completed, consumer_ids_completed)
        logger.debug('master> Sleeping #{}'.format(cycle))
        master_condition.acquire()
        master_condition.wait(30)
        master_condition.release()
        logger.debug('master> Waking up #{}'.format(cycle))

        for worker_name, worker_info in list(workers.items()):
            worker, incoming, control = worker_info
            worker_batch_completed = 0
            worker_done = False
            while True:
                try:
                    worker_status = control.get_nowait()
                except queue.Empty:
                    break
                if worker_status is None:
                    worker_done = True
                    break
                worker_batch_completed += 1
                worker_ids_changed += worker_status[0]
                worker_ids_completed += worker_status[1]
            if worker_done:
                logger.info(
                    'master> Worker "{}" is done #{}'.format(
                        worker_name, cycle))
                worker.join()
                del workers[worker_name]
                continue
            if worker_batch_completed:
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
                except queue.Empty:
                    break
                if consumer_status is None:
                    logger.info(
                        'master> Consumer "{}" is done #{}'.format(
                            consumer.logname, cycle))
                    consumer.join()
                    consumer = None
                    break
                consumer_ids_changed += consumer_status[0]
                consumer_ids_completed += consumer_status[1]

    if worker_ids_completed == ids.total:
        logger.info('master> All done')
    else:
        logger.error(
            'master> Missed {} objects'.format(
                ids.total - worker_ids_completed))
    if worker_ids_changed != worker_ids_completed:
        logger.info(
            'master> Processed {} out of {} ({:.2f}%)'.format(
                worker_ids_changed, worker_ids_completed,
                worker_ids_changed * 100.0 / worker_ids_completed))
    if consumer_ids_changed != consumer_ids_completed:
        logger.info(
            'master> Consumed {} out of {} ({:.2f}%)'.format(
                consumer_ids_changed, consumer_ids_completed,
                consumer_ids_changed * 100.0 / consumer_ids_completed))
