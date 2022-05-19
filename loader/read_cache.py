import configparser as cp
import pandas as pd
import redis
import json
from datetime import datetime

#request_id = '5d244725-dde4-4b3a-9928-63148f88393e'
#request_id = 'c1e5dbee-ae28-46fc-bcf7-1e6e583a706d'
#request_id = '5c77f3ae-a19d-4306-8749-047b21a4dc4d'
#request_id = '33f6888a-363d-42ae-b84b-ce61678ce763'

#print(f'\n\n**************\n\nRequest Id: {request_id}\n\n')

# config
"""
REDIS_HOST = '172.18.0.2'
REDIS_PORT = 6379

cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
"""

def load_request_data(request_id, cache):

    # loading the fields that don't depend on the offer_id, but only on the request_id

    data = []
    request_data = {}

    request_data['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    key_map_1 = {
        "User ID": "user_id",
        "Date Of Birth": "user_profile:birth",
        "city": "user_profile:city",
        "country": "user_profile:country",
        "Profile": "user_profile:profileDefault",
        "Walking speed": "walking_speed",
        "Cycling speed": "cycling_speed",
        "Driving speed": "driving_speed"
    }
    for feature_name, cache_key in key_map_1.items():
        result = cache.get(f'{request_id}:{cache_key}')
        if result:
            request_data[feature_name] = result.decode("utf-8")
        else:
            request_data[feature_name] = 'unknown'

    start_point = cache.get(f'{request_id}:start_point')
    if start_point:
        start_point = json.loads(start_point.decode("utf-8"))['coordinates']
    try:
        start_city = cache.get(f'{request_id}:city_coordinates:{start_point[0]}:{start_point[1]}').decode('utf-8')
    except:
        start_city = 'unknown'

    end_point = cache.get(f'{request_id}:end_point')
    if end_point:
        end_point = json.loads(end_point.decode("utf-8"))['coordinates']
    try:
        end_city = cache.get(f'{request_id}:city_coordinates:{end_point[0]}:{end_point[1]}').decode('utf-8')
    except:
        end_city = 'unknown'

    via_locations = str([start_city])

    request_data['Starting point'] = start_city
    request_data['Destination'] = end_city
    request_data['Via'] = via_locations


    transfers = cache.get(f'{request_id}:max_transfers')
    if transfers:
        request_data['Transfers'] = f'Max {int(transfers.decode("utf-8"))}'
    else:
        request_data['Transfers'] = None

    key_map_2 = {
        'Walking distance to stop': 'walking_dist_to_stop',
        'Cycling distance to stop': 'cycling_dist_to_stop'
    }
    for feature_name, cache_key in key_map_2.items():
        result = cache.get(f'{request_id}:{cache_key}')
        if result:
            request_data[feature_name] = float(result.decode("utf-8"))
        else:
            request_data[feature_name] = 'None'


    key_map_3_a = {
        "Loyalty Card": "user_preferences:Loyalty/Reduction/Payment card",
        "Payment Card": "user_preferences:Payment Card",
        "PRM Type": "user_preferences:PRM type",
        "Preferred means of transportation": "user_preferences:Preferred means of transportation",
        "Preferred carrier": "user_preferences:Preferred carrier",
    }
    key_map_3_b = {
        "Loyalty Card": ['FlyingBlue', 'Grand Voyageur', 'Cartafreccia', 'Golden Card'],
        "Payment Card": ['Apple Wallet', 'Visa', 'Google Wallet', 'Paypal', 'Mastercard'],
        "PRM Type": ['Persons porting a carrycots', 'Pregnant women', 'Persons with deafness or auditory impairments', 'Persons with impairments in their members / users of temporary wheelchair', 'Wheelchair users in mainstreaming seat', 'Older person', 'Persons with blindness or visual impairments'],
        "Preferred means of transportation": ['Metro', 'Trolley Bus', 'Park', 'Cable Way', 'Urban', 'Bus', 'Coach', 'Tram', 'Ship', 'Taxi', 'Intercity', 'Toll', 'Airline', 'Bike Sharing', 'Car Sharing', 'Funicular', 'Train', 'Other'],
        "Preferred carrier": ['Renfe', 'KLM', 'AirFrance', 'VBB', 'Iberia', 'Trenitalia', 'SNFC', 'TMB', 'RegioJet', 'FlixBus'],
    }
    for feature_name in key_map_3_a:
        request_data[feature_name] = str([x for x in key_map_3_b[feature_name]
                                          if cache.get(f'{request_id}:{key_map_3_a[feature_name]}:{x}') == b'1.0'])

    key_map_3bis_a = {
        "Class": "user_preferences:Class",
        "Seat": "user_preferences:Seat",
        "Refund Type": "user_preferences:Refund Type"
    }
    key_map_3bis_b = {
        "Class": ['First class', 'Economy', 'Business'],
        "Seat": ['Large', 'Window', 'Aisle'],
        "Refund Type": ['Manual refund', 'Automatic refund']
    }
    for feature_name in key_map_3bis_a:
        list_result = [x for x in key_map_3bis_b[feature_name]
                       if cache.get(f'{request_id}:{key_map_3bis_a[feature_name]}:{x}') == b'1.0']
        if len(list_result) > 0:
            request_data[feature_name] = list_result[0]
        else:
            request_data[feature_name] = 'unknown'


    #print('\n\n******************\n\nREQUEST DATA')
    #for k in request_data:
    #    print(f'{k}: {request_data[k]}')


    # loading offer-specific fields
    offer_ids = [offer.decode('utf-8')
                 for offer in cache.lrange(f'{request_id}:offers', 0, -1)]
    #print(f'Offer ids: {offer_ids}')

    for offer_id in offer_ids:
        #print(f'\n\n+++++++\nOffer id: {offer_id}')
        offer_data = {}

        offer_data['Travel Offer ID'] = offer_id

        categories = cache.hgetall(f'{request_id}:{offer_id}:categories')
        key_map_4 = {
            "Quick": "quick",
            "Reliable": "reliable",
            "Cheap": "cheap",
            "Comfortable": "comfortable",
            "Door-to-door": "door_to_door",
            "Environmentally friendly": "environmentally_friendly",
            "Short": "short",
            "Multitasking": "multitasking",
            "Social": "social",
            "Panoramic": "panoramic",
            "Healthy": "healthy"
        }
        if categories:
            categories = {k.decode('utf-8'): float(v.decode('utf-8')) for k, v in categories.items()}
            for feature_name, category_key in key_map_4.items():
                if category_key in categories:
                    offer_data[feature_name] = categories[category_key]
                else:
                    offer_data[feature_name] = None
        else:
            for feature_name, category_key in key_map_4.items():
                offer_data[feature_name] = None

        key_map_5 = {
            "Departure time": "start_time",
            "Arrival time": "end_time",
        }
        for feature_name, cache_key in key_map_5.items():
            result = cache.get(f'{request_id}:{offer_id}:{cache_key}')
            if result:
                offer_data[feature_name] = result.decode("utf-8")
            else:
                offer_data[feature_name] = None
        offer_data['Transfer duration'] = 'normal'

        legs_number = cache.get(f'{request_id}:trip_request:leg_information:{offer_id}:num_legs')
        if legs_number:
            offer_data['Legs Number'] = int(legs_number.decode('utf-8'))

        leg_ids = [leg.decode('utf-8')
                   for leg in cache.lrange(f'{request_id}:{offer_id}:legs', 0, -1)]
        #print(f'Leg ids: {leg_ids}')

        key_map_6 = {
            "LegMode": "transportation_mode",
            "LegCarrier": "leg_carrier",
            "LegSeat": "seating_quality"
        }
        for feature_name in key_map_6:
            offer_data[feature_name] = []
        offer_data['LegLength'] = []

        for leg_id in leg_ids:
            #print(f'\nLeg id: {leg_id}')

            for feature_name, cache_key in key_map_6.items():
                result = cache.get(f'{request_id}:{offer_id}:{leg_id}:{cache_key}')
                if result:
                    offer_data[feature_name].append(result.decode("utf-8"))
                else:
                    offer_data[feature_name].append(None)
            leg_length = cache.get(f'{request_id}:{offer_id}:{leg_id}:leg_length')
            if leg_length:
                offer_data['LegLength'].append(float(leg_length.decode('utf-8')))
            else:
                offer_data['LegLength'].append(None)

        for feature_name in ["LegMode", "LegCarrier", "LegSeat", "LegLength"]:
            offer_data[feature_name] = str(offer_data[feature_name])

        offer_data['Services'] = '[]'     # missing
        #print('\n\n******************\n\nOFFER DATA')
        #for k in offer_data:
        #    print(f'{k}: {offer_data[k]}')

        offer_data.update(request_data)
        data.append(offer_data)

    df = pd.DataFrame(data)
    return df
