#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import csv
import json
import pickle
import hashlib
import pathlib
import argparse
from pprint import pprint
from datetime import datetime

import redis

from r2r_offer_utils.cli_utils import IntRange


NPRINT = 10
REDIS_PORT = 6382
REDIS_HOST = "localhost"


SAMPLE_BYTE = 1024
def csv_has_header(fp):
    sniffer = csv.Sniffer()
    has_header = sniffer.has_header(fp.read(SAMPLE_BYTE))
    fp.seek(0)

    return has_header


key_map = {
    "Travel Offer ID"                       : "offer_id",
    "User ID"                               : "user_id",
    "TimeStamp"                             : "timestamp",
    "Date Of Birth"                         : "birth_date",
    "city"                                  : "city",
    "country"                               : "country",
    "Loyalty Card"                          : "loyalt_card",
    "Payment Card"                          : "payment_card",
    "PRM Type"                              : "prm_type",
    "Preferred means of transportation"     : "preferred_mot",
    "Preferred carrier"                     : "preferred_carrier",
    "Class"                                 : "class",
    "Seat"                                  : "seat",
    "Refund Type"                           : "refund_type",
    "Quick"                                 : "quick",
    "Reliable"                              : "reliable",
    "Cheap"                                 : "cheap",
    "Comfortable"                           : "comfortable",
    "Door-to-door"                          : "door_to_door",
    "Environmentally friendly"              : "env_friendly",
    "Short"                                 : "short",
    "Multitasking"                          : "multitasking",
    "Social"                                : "social",
    "Panoramic"                             : "panoramic",
    "Healthy"                               : "healthy",
    "Legs Number"                           : "leg_number",
    "Profile"                               : "profile",
    "Starting point"                        : "starting_point",
    "Destination"                           : "destination",
    "Via"                                   : "via",
    "LegMode"                               : "leg_mode",
    "LegCarrier"                            : "leg_carrier",
    "LegSeat"                               : "leg_seat",
    "LegLength"                             : "leg_length",
    "Departure time"                        : "departure_time",
    "Arrival time"                          : "arrival_time",
    "Services"                              : "services",
    "Transfers"                             : "transfers",
    "Transfer duration"                     : "transfer_duration",
    "Walking distance to stop"              : "walking_distance_to_stop",
    "Walking speed"                         : "walking_speed",
    "Cycling distance to stop"              : "cycling_distance_to_stop",
    "Cycling speed"                         : "cycling_speed",
    "Driving speed"                         : "driving_speed",
    "Bought Tag"                            : "bought_tag",
}
reverse_key_map = dict((v, k) for k, v in key_map.items())

identity = lambda x: x


def hash_string(astring):
    hasher = hashlib.sha1()
    hasher.update(astring.encode('utf-8'))

    hashed = hasher.hexdigest()

    return hashed


def convert_list(astring):
    return eval(astring)

def convert_time(time_format):
    def __convert_time(astring):
        date = datetime.strptime(astring, time_format)
        return date.isoformat(timespec='seconds')

    return __convert_time


def convert_distance(astring):
    return int(astring.strip('m'))


field_map = {
    "offer_id"                              : hash_string,
    "user_id"                               : hash_string,
    "timestamp"                             : convert_time("%Y-%m-%d %H:%M:%S.%f"),
    "birth_date"                            : identity,
    "city"                                  : identity,
    "country"                               : identity,
    "loyalt_card"                           : convert_list,
    "payment_card"                          : convert_list,
    "prm_type"                              : convert_list,
    "preferred_mot"                         : convert_list,
    "preferred_carrier"                     : convert_list,
    "class"                                 : identity,
    "seat"                                  : identity,
    "refund_type"                           : identity,
    "quick"                                 : float,
    "reliable"                              : float,
    "cheap"                                 : float,
    "comfortable"                           : float,
    "door_to_door"                          : float,
    "env_friendly"                          : float,
    "short"                                 : float,
    "multitasking"                          : float,
    "social"                                : float,
    "panoramic"                             : float,
    "healthy"                               : float,
    "leg_number"                            : int,
    "profile"                               : identity,
    "starting_point"                        : identity,
    "destination"                           : identity,
    "via"                                   : convert_list,
    "leg_mode"                              : convert_list,
    "leg_carrier"                           : convert_list,
    "leg_seat"                              : convert_list,
    "leg_length"                            : convert_list,
    "departure_time"                        : convert_time("%Y-%m-%d %H:%M"),
    "arrival_time"                          : convert_time("%Y-%m-%d %H:%M"),
    "services"                              : convert_list,
    "transfers"                             : identity,
    "transfer_duration"                     : identity,
    "walking_distance_to_stop"              : convert_distance,
    "walking_speed"                         : identity,
    "cycling_distance_to_stop"              : identity,
    "cycling_speed"                         : float,
    "driving_speed"                         : float,
    "bought_tag"                            : bool,
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files',
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

    args = parser.parse_args()

    print("Connecting to Redis instance on {host}:{port}..."
          .format(host=args.host, port=args.port),
          file=sys.stderr, flush=True)
    redis = redis.Redis(host=args.host, port=args.port)

    # converted_files = {}
    for indir in args.input_files:
        # converted_files[indir.as_posix()] = {}
        # converted = converted_files[indir.as_posix()]
        converted = {}

        with indir.open('r') as infp:
            reader = csv.DictReader(infp, fieldnames=key_map.keys())

            if csv_has_header(infp):
                next(reader)

            converted["request_id"] = hash_string(indir.name)
            for row in reader:
                pprint(row)
                for key, value in row.items():
                    new_key = key_map[key]
                    new_value = field_map[new_key](value)

                    converted[new_key] = new_value

        # start_time
        reqid = converted["request_id"]
        # for key, value in converted.items():
        #     redis.set(f"{reqid}:{key}", converted[key])

        # "offer_id"
        redis.set(f"{reqid}:{key}", converted[key])
        # "user_id"
        redis.set(f"{reqid}:{key}", converted[key])
        # "timestamp"
        redis.set(f"{reqid}:{key}", converted[key])
        # "birth_date"
        redis.set(f"{reqid}:{key}", converted[key])
        # "city"
        redis.set(f"{reqid}:{key}", converted[key])
        # "country"
        redis.set(f"{reqid}:{key}", converted[key])
        # "loyalt_card"
        redis.set(f"{reqid}:{key}", converted[key])
        # "payment_card"
        redis.set(f"{reqid}:{key}", converted[key])
        # "prm_type"
        redis.set(f"{reqid}:{key}", converted[key])
        # "preferred_mot"
        redis.set(f"{reqid}:{key}", converted[key])
        # "preferred_carrier"
        redis.set(f"{reqid}:{key}", converted[key])
        # "class"
        redis.set(f"{reqid}:{key}", converted[key])
        # "seat"
        redis.set(f"{reqid}:{key}", converted[key])
        # "refund_type"
        redis.set(f"{reqid}:{key}", converted[key])
        # "quick"
        redis.set(f"{reqid}:{key}", converted[key])
        # "reliable"
        redis.set(f"{reqid}:{key}", converted[key])
        # "cheap"
        redis.set(f"{reqid}:{key}", converted[key])
        # "comfortable"
        redis.set(f"{reqid}:{key}", converted[key])
        # "door_to_door"
        redis.set(f"{reqid}:{key}", converted[key])
        # "env_friendly"
        redis.set(f"{reqid}:{key}", converted[key])
        # "short"
        redis.set(f"{reqid}:{key}", converted[key])
        # "multitasking"
        redis.set(f"{reqid}:{key}", converted[key])
        # "social"
        redis.set(f"{reqid}:{key}", converted[key])
        # "panoramic"
        redis.set(f"{reqid}:{key}", converted[key])
        # "healthy"
        redis.set(f"{reqid}:{key}", converted[key])
        # "leg_number"
        redis.set(f"{reqid}:{key}", converted[key])
        # "profile"
        redis.set(f"{reqid}:{key}", converted[key])
        # "starting_point"
        redis.set(f"{reqid}:{key}", converted[key])
        # "destination"
        redis.set(f"{reqid}:{key}", converted[key])
        # "via"
        redis.set(f"{reqid}:{key}", converted[key])
        # "leg_mode"
        redis.set(f"{reqid}:{key}", converted[key])
        # "leg_carrier"
        redis.set(f"{reqid}:{key}", converted[key])
        # "leg_seat"
        redis.set(f"{reqid}:{key}", converted[key])
        # "leg_length"
        redis.set(f"{reqid}:{key}", converted[key])
        # "departure_time"
        redis.set(f"{reqid}:{key}", converted[key])
        # "arrival_time"
        redis.set(f"{reqid}:{key}", converted[key])
        # "services"
        redis.set(f"{reqid}:{key}", converted[key])
        # "transfers"
        redis.set(f"{reqid}:{key}", converted[key])
        # "transfer_duration"
        redis.set(f"{reqid}:{key}", converted[key])
        # "walking_distance_to_stop"
        redis.set(f"{reqid}:{key}", converted[key])
        # "walking_speed"
        redis.set(f"{reqid}:{key}", converted[key])
        # "cycling_distance_to_stop"
        redis.set(f"{reqid}:{key}", converted[key])
        # "cycling_speed"
        redis.set(f"{reqid}:{key}", converted[key])
        # "driving_speed"
        redis.set(f"{reqid}:{key}", converted[key])
        # "bought_tag"
        redis.set(f"{reqid}:{key}", converted[key])

    print('All data loaded!', file=sys.stderr)
    exit(0)
