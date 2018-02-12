import argparse
import os
import sys
import pkg_resources
import shutil

import ZODB.blob
import ZODB.utils
import mdtools.relstorage.reference


def list_all_blobs_in(base_dir):
    blobs = set()
    if not base_dir:
        return blobs
    trim_size = len(base_dir.rstrip(os.path.sep)) + 1
    for (directory, _, filenames) in os.walk(base_dir):
        if not filenames or '.layout' in filenames:
            continue
        blobs.add(directory[trim_size:])
    return blobs


def read_manifest(manifest):
    blobs = set()
    with open(manifest, 'r') as stream:
        for line in stream.readlines():
            line = line.strip()
            if not len(line):
                continue
            blob = os.path.dirname(line)
            if not (len(blob) and blob.startswith('0x00')):
                print('This does not look like a blob directory to me.')
                sys.exit(-1)
            blobs.add(blob)
    return blobs


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description=(
            'Generate an SQL file with delete statements to remove '
            'unused objects.'))
    parser.add_argument(
        '--references', metavar='FILE.DB', dest='refsdb',
        help='reference information computed by zodbcheck')
    parser.add_argument(
        '--blobs', metavar='BLOBS', dest='blobs',
        help='directory where blobs are stored')
    parser.add_argument(
        '--blobs-manifest', metavar='MANIFEST',
        dest='blobs_manifest',
        help='result of "find 0x00 -type f" in blobs directory')
    parser.add_argument(
        '--lines', metavar='NUMBER', dest='lines', type=int,
        help='Number of lines per file', default=50000)
    parser.add_argument(
        '--output', metavar='OUTPUT', dest='output',
        help='Output directory', default='pack')
    args = parser.parse_args(args)
    try:
        references = mdtools.relstorage.reference.Database(args.refsdb)
    except ValueError as error:
        parser.error(error.args[0])
    if args.blobs_manifest:
        blobs = read_manifest(args.blobs_manifest)
    else:
        blobs = list_all_blobs_in(args.blobs)
    compute_blob = None
    count_oid = 0
    count_blobs = 0
    filename_count = 1
    sql = None
    os.makedirs(args.output)
    shutil.copyfile(
        pkg_resources.resource_filename('mdtools.relstorage', 'sql.sh'),
        os.path.join(os.path.join(args.output, 'sql.sh')))
    os.makedirs(os.path.join(args.output, 'todo'))
    if blobs:
        compute_blob = ZODB.blob.BushyLayout().oid_to_path
        shell = open(os.path.join(args.output, 'blobs.sh'), 'w')
        shell.write('#!/usr/bin/env bash\n')
        shell.write('if ! test -d 0x00; then\n')
        shell.write('   echo "Wrong directory."; exit 1\n')
        shell.write('fi\n')
    else:
        print('Warning: no blobs detected.')
    for oid in references.get_unused_oids():
        count_oid += 1
        if sql is None:
            sql = open(os.path.join(
                args.output,
                'todo',
                'pack-{:06}.sql'.format(filename_count)), 'w')
            filename_count += 1
            sql.write('BEGIN;\n')
        sql.write('DELETE FROM object_state WHERE zoid = {};\n'.format(oid))
        if count_oid and count_oid % args.lines == 0:
            sql.write('COMMIT;\n')
            sql.close()
            sql = None
        if compute_blob is not None:
            blob = compute_blob(ZODB.utils.p64(oid))
            if blob in blobs:
                count_blobs += 1
                blobs.remove(blob)
                shell.write('rm -rf {}\n'.format(blob))
    if sql is not None:
        sql.write('COMMIT;\n')
        sql.close()
    if compute_blob is not None:
        shell.close()
    print('Found {} objects and {} blobs.'.format(count_oid, count_blobs))
