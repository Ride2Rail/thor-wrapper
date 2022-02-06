# -*- coding: utf-8 -*-

import os
import random
import operator
import pathlib

import numpy as np
import pandas as pd


def init(config):

    def split_list(alist, separator=','):
        return [i.strip() for i in alist.split(separator)]

    global execution_mode
    execution_mode = config['running']['mode']

    filename = pathlib.Path(__file__).name
    print(f"execution_mode ({filename}): {execution_mode}")

    global classifiers_path
    global clusters_path
    global travel_offers_path
    global user_profiles_path
    classifiers_path = pathlib.Path(config['thor.paths']['classifiers_path'])
    clusters_path = pathlib.Path(config['thor.paths']['clusters_path'])
    travel_offers_path = pathlib.Path(config['thor.paths']['travel_offers_path'])
    user_profiles_path = pathlib.Path(config['thor.paths']['user_profiles_path'])


def rank(user, travel_offers):
    print(f"user: {user} - travel_offers: {travel_offers}")
    np.random.seed(seed=5509)

    ranked_offers = {}

    noffers = len(travel_offers)
    scores =  [round(num, 3) for num in np.random.rand(noffers).tolist()]

    offer_by_score = enumerate(sorted(zip(travel_offers, scores),
                                      key=operator.itemgetter(1),
                                      reverse=True
                                      ),
                               start=1
                               )


    for rank, (offer_id, score) in offer_by_score:
        ranked_offers[offer_id] = {"rank": rank, "score": score}

    return ranked_offers


def read_offers(offer_id):
    t_off = pd.read_csv(travel_offers_path/(offer_id + '.csv'))
    return t_off


def read_user_data(user_id):
    user_profile_file = user_profiles_path/(user_id + '.csv')
    user_data = pd.read_csv(user_profile_file)

    user_data['User ID'] = user_id
    return user_data


def get_user_model(user):
    print("open classifier:", os.path.join(classifiers_path, user + '_classifier.pkl'))
    model, model_col = None, None

    if pathlib.Path(classifiers_path, user+'_classifier.pkl').exists():
        model = pickle.load(open(os.path.join(classifiers_path, user + '_classifier.pkl'), 'rb'))
        with open(os.path.join(classifiers_path, user + '_columns.txt'), 'r') as f:
            model_col = [i.split('\n')[0] for i in f.readlines()]

    return model, model_col


if __name__ == '__main__':
    import ipdb; ipdb.set_trace()
