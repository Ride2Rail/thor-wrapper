# -*- coding: utf-8 -*-

import os
import random
import pickle
import pathlib
import operator

import pandas as pd
import sklearn
import sklearn.preprocessing

from preprocessing import preprocessing_user_profile


def init(config):
    global execution_mode

    execution_mode = config.get('running', 'mode')
    print("execution_mode:", execution_mode)

    global rank
    global read_offers
    global classifier_path
    global travel_offers_path
    global classifier_path
    global travel_offers_path

    global users_columns
    global one_hot_col_cat
    global one_hot_col_cat_list
    global classifier_columns
    global clustering_columns
    global target_column

    users_columns = [i.strip() for i in config['thor.columns']['users_columns'].split(',')]
    one_hot_col_cat = [i.strip() for i in config['thor.columns']['one_hot_categorical_columns'].split(',')]
    one_hot_col_cat_list = [i.strip() for i in config['thor.columns']['one_hot_categorical_list_columns'].split(',')]
    classifier_columns = [i.strip() for i in config['thor.columns']['classifier_columns'].split(',')]
    clustering_columns = [i.strip() for i in config['thor.columns']['clustering_columns'].split(',')]
    target_column = config['thor.columns']['target_column']

    if execution_mode == 'PRODUCTION':
        rank = rank_prod
        read_offers = read_offers_prod
        classifier_path = config.get('thor', 'classifier_path')
        travel_offers_path = config.get('thor', 'travel_offers_path')
    else:
        rank = rank_prod
        read_offers = read_offers_dev
        classifier_path = pathlib.Path('data/users_classifier/')
        travel_offers_path = pathlib.Path('data/travel_offers/')


def rank_dev(user, travel_offers):
    import numpy as np
    np.random.seed(seed=5509)

    ranked_offers = {}

    noffers = len(travel_offers)
    scores =  np.random.rand(noffers).tolist() 

    offer_by_score = enumerate(sorted(zip(travel_offers, scores),
                                      key=operator.itemgetter(1),
                                      reverse=True
                                      ),
                               start=1
                               )


    for rank, (offer_id, score) in offer_by_score:
        ranked_offers[offer_id] = {"rank": rank, "score": score}

    return ranked_offers


def read_offers_dev(offer_id):
    t_off = pd.read_csv(travel_offers_path / (offer_id + '.csv'))
    return t_off

def read_offers_prod(offer_id):
    return offer_id


# task 4
def rank_prod(user, travel_offers):
    ranked_offers = {}

    classifier_model = pickle.load(open(os.path.join(classifier_path, user + '_classifier.pkl'), 'rb'))
    with open(os.path.join(classifier_path, user + '_columns.txt'), 'r') as f:
        classifier_model_col = [i.split('\n')[0] for i in f.readlines()]

    offer_scores = {}
    offer_by_score = {}
    for offer_id in travel_offers:
        t_off = read_offers(offer_id)

        new_t_off, new_columns = preprocessing_user_profile(t_off, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_t_off[new_t_off.columns.intersection(all_class_cols)]

        columns_n = list(classifier_df.columns.values)
        classifier_np = classifier_df.values
        min_max_scaler = sklearn.preprocessing.MinMaxScaler()
        classifier_np_scaled = min_max_scaler.fit_transform(classifier_np)
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns_n, index=range(len(classifier_df)))

        common_cols = list(set(classifier_df.columns.values).intersection(set(classifier_model_col)))
        uncommon_cols = list(set(classifier_model_col) - set(classifier_df.columns.values))

        classifier_df2 = pd.DataFrame(columns=classifier_model_col, index=range(len(classifier_df)))
        for offer_df_id in range(len(classifier_df)):
            classifier_df2.loc[offer_df_id, common_cols] = classifier_df.loc[offer_df_id, common_cols]
            classifier_df2.loc[offer_df_id, uncommon_cols] = 0

        pred = classifier_model.predict(classifier_df2)
        class_sco = classifier_model.score(classifier_df2, pred)
        print(f"{offer_id}: {class_sco}")
        offer_scores[offer_id] = class_sco

    offer_by_score = enumerate(sorted(offer_scores.items(),
                                      key=operator.itemgetter(1),
                                      reverse=True
                                      ),
                               start=1
                               )
    for rank, (offer_id, score) in offer_by_score:
        ranked_offers[offer_id] = {"rank": rank, "score": score}

    return ranked_offers
