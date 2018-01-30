import io
import contextlib
import base64
import psycopg2
import zodbupdate.convert
import zodbupdate.serialize
import zodbupdate.main

BATCH_SIZE = 200000

logger = zodbupdate.main.setup_logger()


@contextlib.contextmanager
def new_connection(dsn):
    connection = psycopg2.extensions.connection(dsn)
    cursor = connection.cursor()
    cursor.arraysize = 64
    yield cursor
    connection.commit()
    connection.close()


def ids_batch(dsn):
    offset = 0
    while True:
        with new_connection(dsn) as cursor:
            cursor.execute(
                "SELECT zoid FROM object_state "
                "ORDER BY zoid LIMIT %s OFFSET %s",
                (BATCH_SIZE, offset))
            oids = [result[0] for result in cursor.fetchall()]
        if not oids:
            break
        yield oids
        offset += BATCH_SIZE


def read_batch(dsn, ids_batch):
    for index, oids in enumerate(ids_batch):
        logger.info('Read data #{}'.format(index + 1))
        batch = []
        with new_connection(dsn) as cursor:
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


def write_batch(dsn, processor):
    for index, batch in enumerate(processor):
        logger.info('Write data #{}'.format(index + 1))
        with new_connection(dsn) as cursor:
            cursor.executemany(
                "UPDATE object_state SET state = decode(%s, 'base64') "
                "WHERE zoid = %s",
                batch)


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


def process_batch(read_batch):
    processor = create_processor()
    for index, incoming_batch in enumerate(read_batch):
        logger.info('Processing #{}'.format(index+1))
        result_batch = []
        for data, oid in incoming_batch:
            output_file = processor.rename(decode_record(data))
            if output_file is not None:
                result_batch.append((encode_record(output_file), oid))
        yield result_batch


def main():
    dsn = "dbname='maas_dev'"
    write_batch(dsn, process_batch(read_batch(dsn, ids_batch(dsn))))
