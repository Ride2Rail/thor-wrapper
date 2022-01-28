#!/usr/bin/env python3

import os
import random
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger
from r2r_offer_utils.cache_operations import read_data_from_cache_wrapper, store_simple_data_to_cache_wrapper
from r2r_offer_utils.normalization import zscore, minmaxscore

import json
import requests

service_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(service_name)

# config
config = cp.ConfigParser()
config.read(f'{service_name}.conf')

# logging
logger, ch = setup_logger()

# cache
cache = redis.Redis(host=config.get('cache', 'host'),
                    port=config.get('cache', 'port'),
                    decode_responses=True)


execution_mode = config.get('running', 'mode')


# task 1: make a classifier for every user
@app.route('/classify-all', methods=['POST'])
def classify_all():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

 
    return response


# task 2: cluster all the users exist in user profile folder
@app.route('/cluster-all', methods=['POST'])
def cluster_all():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

 
    return response


# task 3: find a cluster for one or more new users
@app.route('/cluster-one', methods=['POST'])
def cluster_one():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

 
    return response


# task 4: rank travel offers, for a given user
@app.route('/rank', methods=['POST'])
def rank():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)

    result = {}
    result["request_id"] = request_id
    result["offers"] = {}

    ranks = list(range(len(offer_data)))
    random.shuffle(ranks)
    for offer_id, rank in zip(offer_data, ranks):
        result["offers"][offer_id.decode()] = rank

    json_object = json.dumps(result) 
    response = app.response_class(
        response=json_object,
        status=200,
        mimetype='application/json'
    )
 
    return response


if __name__ == '__main__':
    import argparse
    import logging
    from r2r_offer_utils.cli_utils import IntRange

    FLASK_PORT = 5000
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-host',
                        default=REDIS_HOST,
                        help=f'Redis hostname [default: {REDIS_HOST}].')
    parser.add_argument('--redis-port',
                        default=REDIS_PORT,
                        type=IntRange(1, 65536),
                        help=f'Redis port [default: {REDIS_PORT}].')
    parser.add_argument('--flask-port',
                        default=FLASK_PORT,
                        type=IntRange(1, 65536),
                        help=f'Flask port [default: {FLASK_PORT}].')

    args = parser.parse_args()

    # remove default logger
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    # create file handler which logs debug messages
    fh = logging.FileHandler(f"{service_name}.log", mode='a+')
    fh.setLevel(logging.DEBUG)

    # set logging level to debug
    ch.setLevel(logging.DEBUG)

    os.environ["FLASK_ENV"] = "development"

    cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    app.run(port=FLASK_PORT, debug=True)

    exit(0)
