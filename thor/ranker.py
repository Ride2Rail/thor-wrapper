# -*- coding: utf-8 -*-

import pickle
import operator
import pathlib

import pandas as pd
import sklearn
import sklearn.tree
import sklearn.preprocessing

import dev
import utils
from preprocessing import preprocessing_user_profile


def init(config):
    def split_list(alist, sep=','):
        return [el.strip() for el in alist.split(sep)]

    execution_mode = config['running']['mode']
    filename = pathlib.Path(__file__).name
    print(f"execution_mode ({filename}): {execution_mode}")

    dev.init(config)
    utils.init(config)

    global user_columns
    global one_hot_col_cat
    global one_hot_col_cat_list
    global classifier_columns
    global clustering_columns
    user_columns = split_list(config['thor.columns']['user_columns'])
    one_hot_col_cat = split_list(config['thor.columns']['one_hot_categorical_columns'])
    one_hot_col_cat_list = split_list(config['thor.columns']['one_hot_categorical_list_columns'])
    classifier_columns = split_list(config['thor.columns']['classifier_columns'])
    clustering_columns = split_list(config['thor.columns']['clustering_columns'])


    global read_offers
    global get_user_model
    global read_user_data
    global write_user_model
    global write_cluster
    if execution_mode == 'PRODUCTION':
        read_offers = utils.read_offers
        get_user_model = utils.get_user_model_cache
        read_user_data = utils.read_user_data
        write_user_model = utils.write_user_model
        write_cluster = utils.write_cluster
    else:
        read_offers = dev.read_offers
        get_user_model = dev.get_user_model
        read_user_data = dev.read_user_data
        write_user_model = dev.write_user_model
        write_cluster = dev.write_cluster


# task 3
def cluster_user(user):
    users_dict = {}
    clu_columns = []

    print(f"cluster_user({user})")
    clustering_df = pd.DataFrame(columns=user_columns, index=range(len(user)))
    user_data = read_user_data(user)

    clustering_df.loc[user, :] = user_data.loc[0, :]

    new_user_data, new_columns = preprocessing_user_profile(clustering_df, one_hot_col_cat, one_hot_col_cat_list)
    all_clus = [clu_columns.extend([i for i in new_columns if clu_col in i]) for clu_col in clustering_columns]
    clu_columns = list(set(clu_columns))
    clu_columns += clustering_columns
    clustering_df = new_user_data[new_user_data.columns.intersection(clu_columns)]

    columns_n = list(clustering_df.columns.values)
    clustering_np = clustering_df.values
    min_max_scaler = preprocessing.MinMaxScaler()
    clustering_np_scaled = min_max_scaler.fit_transform(clustering_np)
    clustering_df = pd.DataFrame(data=clustering_np_scaled, columns=columns_n, index=range(len(clustering_df)))

    write_cluster(cluster_model, cluster_model_col)

    common_cols = list(set(clustering_df.columns.values).intersection(set(cluster_model_col)))
    uncommon_cols = list(set(cluster_model_col) - set(clustering_df.columns.values))

    clustering_df2 = pd.DataFrame(columns=cluster_model_col, index=range(len(user)))
    for user_id in range(len(user)):
        clustering_df2.loc[user_id, common_cols] = clustering_df.loc[user_id, common_cols]
        clustering_df2.loc[user_id, uncommon_cols] = 0

    cl_label = cluster_model.predict(clustering_df2)

    label = cl_label[user_id]
    user_classifier, user_classifier_col = write_user_model(label)

    return user_classifier, user_classifier_col


# task 4
def rank(user, travel_offers):
    ranked_offers = {}

    classifier_model, classifier_model_col = get_user_model(user)
    print(f"user: {user} - classifier_model: {classifier_model}")
    if classifier_model is None:
        classifier_model, classifier_model_col = cluster_user(user)

    offer_scores = {}
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
