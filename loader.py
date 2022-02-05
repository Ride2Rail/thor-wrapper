#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import pickle
import hashlib
import pathlib
import argparse

import redis

from r2r_offer_utils.cli_utils import IntRange


NPRINT = 10
REDIS_PORT = 6380
REDIS_HOST = "localhost"


def hash_name(filepath):
    hasher = hashlib.sha1()
    hasher.update(filepath.name.encode('utf-8'))

    hashed = hasher.hexdigest()

    return hashed


def load_pickle_from_file(pickle_path):
    with pickle_path.open('rb') as pickle_file:
        pickled_object = pickle_file.read()

    return pickled_object


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_directories',
                        metavar='<directory>',
                        nargs='+',
                        type=pathlib.Path,
                        help="Data directory file to load")
    parser.add_argument('-H', '--host',
                        default="localhost",
                        help=f"Redis hostname [default: {REDIS_HOST}].")
    parser.add_argument('-p', '--port',
                        default=REDIS_PORT,
                        type=IntRange(1, 65536),
                        help=f'Redis port [default: {REDIS_PORT}].')
    parser.add_argument('-s', '--suffix',
                        nargs='*',
                        help='Suffix to use to the key.')

    args = parser.parse_args()

    objects = {}
    stems = {}
    print("Reading input...", file=sys.stderr, flush=True)
    for indir in args.input_directories:
        objects[indir.as_posix()] = list(indir.rglob("*.pkl"))

    for idx, indir in enumerate(args.input_directories):
        if args.suffix and idx < len(args.suffix):
            stems[indir.as_posix()] = args.suffix[idx]
        else:
            stems[indir.as_posix()] = indir.stem

    print("Connecting to Redis instance on {host}:{port}..."
          .format(host=args.host, port=args.port),
          file=sys.stderr, flush=True)
    redis = redis.Redis(host=args.host, port=args.port)

    obj_count = 0
    for path, pickles in objects.items():
        suffix = stems[path]
        print(f"* path: {path} ({suffix})")

        for pkl in pickles:
            pklid = hash_name(pkl)
            pickled_object = load_pickle_from_file(pkl)
            # object = pickle.loads(pickled_object)

            key = f"{pklid}:{suffix}"
            print(f"  - {pkl}")
            print(f"    {key}")
            redis.set(key, pickled_object)
            obj_count = obj_count + 1

            if obj_count % NPRINT == 0:
                print(f'{obj_count} objects read', file=sys.stderr, flush=True)

    print('All data loaded!', file=sys.stderr)
    exit(0)
