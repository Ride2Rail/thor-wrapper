from clustering import *

# task 1
def make_classifier_for_user(users_classifying, classifier_columns, users_profiles_path, target_column, one_hot_col_cat,
                             one_hot_col_cat_list, users_classifier_path):

    for prof in users_classifying:

        user_add = os.path.join(users_profiles_path, prof) + '.csv'

        user_data = pd.read_csv(user_add)
        new_user_data, new_columns = preprocessing_user_profile(user_data, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_user_data[new_user_data.columns.intersection(all_class_cols)]

        columns = list(classifier_df.columns.values)
        # print(len(columns))
        classifier_np = classifier_df.values
        min_max_scaler = preprocessing.MinMaxScaler()
        classifier_np_scaled = min_max_scaler.fit_transform(classifier_np)
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns, index=range(len(classifier_df)))

        nunique = classifier_df.nunique()
        cols_to_drop = nunique[nunique == 1].index
        classifier_df2 = classifier_df.drop(cols_to_drop, axis=1)

        classifiers_grid_search(classifier_df2, users_classifier_path, prof.split('.csv')[0], target_col=target_column)


def make_classifier_for_all_users(classifier_columns, users_profiles_path, target_column, one_hot_col_cat,
                                  one_hot_col_cat_list, classifier_path, classifier_columns_path):

    dataframes = []
    for user_file in os.listdir(users_profiles_path):

        user_data = pd.read_csv(os.path.join(users_profiles_path, user_file))
        new_user_data, new_columns = preprocessing_user_profile(user_data, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_user_data[new_user_data.columns.intersection(all_class_cols)]

        columns = list(classifier_df.columns.values)
        # print(len(columns))
        classifier_np = classifier_df.values
        min_max_scaler = preprocessing.MinMaxScaler()
        classifier_np_scaled = min_max_scaler.fit_transform(classifier_np)
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns, index=range(len(classifier_df)))

        nunique = classifier_df.nunique()
        cols_to_drop = nunique[nunique == 1].index
        classifier_df2 = classifier_df.drop(cols_to_drop, axis=1)
        dataframes.append(classifier_df2)

    df_all_users = pd.concat(dataframes, ignore_index=True)
    df_all_users = df_all_users.fillna(0)
    classifiers_grid_search_all(df_all_users, classifier_path, classifier_columns_path, target_col=target_column)


# task 2
def cluster_users(users_columns, users_profiles_path, clustering_columns, one_hot_col_cat, one_hot_col_cat_list,
                  classifier_columns, clusters_path):

    all_users = sorted([i for i in os.listdir(users_profiles_path) if '.csv' in i])

    clustering_df = pd.DataFrame(columns=users_columns, index=range(len(all_users)))
    users_dict = {}
    clu_columns = []
    for user_id in range(len(all_users)):
        user = all_users[user_id]
        user_add = os.path.join(users_profiles_path, user)
        user_data = pd.read_csv(user_add)
        clustering_df.loc[user_id, :] = user_data.loc[0, :]
        users_dict[user_id] = user_data.loc[0, 'User ID']
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

    nunique = clustering_df.nunique()
    cols_to_drop = nunique[nunique == 1].index
    clustering_df2 = clustering_df.drop(cols_to_drop, axis=1)

    clustering_grid_search(clustering_df2, clusters_path, users_columns, users_dict, classifier_columns,
                           users_profiles_path, one_hot_col_cat, one_hot_col_cat_list,
                           target_col='Bought Tag')

# task 3
def new_user_cluster_pred(new_users_clustering, clusters_path, users_columns, clustering_columns,
                          users_profiles_path, one_hot_col_cat, one_hot_col_cat_list, users_classifier_path):

    clustering_df = pd.DataFrame(columns=users_columns, index=range(len(new_users_clustering)))
    users_dict = {}
    clu_columns = []
    for user_id in range(len(new_users_clustering)):
        user = new_users_clustering[user_id]
        user_add = os.path.join(users_profiles_path, user)
        user_data = pd.read_csv(user_add)
        clustering_df.loc[user_id, :] = user_data.loc[0, :]
        users_dict[user_id] = user_data.loc[user_id, 'User ID']
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

    cluster_model = pickle.load(open(os.path.join(clusters_path, 'cluster.pkl'), 'rb'))
    with open(os.path.join(clusters_path, 'cluster.txt'), 'r') as f:
        cluster_model_col = [i.split('\n')[0] for i in f.readlines()]

    common_cols = list(set(clustering_df.columns.values).intersection(set(cluster_model_col)))
    uncommon_cols = list(set(cluster_model_col) - set(clustering_df.columns.values))

    clustering_df2 = pd.DataFrame(columns=cluster_model_col, index=range(len(new_users_clustering)))
    for user_id in range(len(new_users_clustering)):
        clustering_df2.loc[user_id, common_cols] = clustering_df.loc[user_id, common_cols]
        clustering_df2.loc[user_id, uncommon_cols] = 0

    cl_label = cluster_model.predict(clustering_df2)

    for user_id in range(len(new_users_clustering)):
        user = new_users_clustering[user_id]
        label = cl_label[user_id]
        user_classifier = pickle.load(open(os.path.join(clusters_path, 'cluster_' + str(label) + '_classifier.pkl'), 'rb'))
        with open(os.path.join(clusters_path, 'cluster_' + str(label) + '_columns.txt'), 'r') as f:
            user_classifier_col = [i.split('\n')[0] for i in f.readlines()]

        file_name = os.path.join(users_classifier_path, user + "_classifier.pkl")
        col_file_name = os.path.join(users_classifier_path, user + "_columns.txt")
        with open(file_name, "wb") as open_file:
            pickle.dump(user_classifier, open_file)

        with open(col_file_name, 'w') as f:
            for elem in user_classifier_col:
                f.write(elem + '\n')

# task 4
def sort_offers(request_df, classifier_path, classifier_columns_path, one_hot_col_cat, one_hot_col_cat_list, classifier_columns):
    request_df = request_df.reindex(sorted(request_df.columns), axis=1)
    buying_prob = []
    not_buying_prob = []
    classifier_model = pickle.load(open(classifier_path, 'rb'))
    with open(classifier_columns_path, 'r') as f:
        classifier_model_col = [i.split('\n')[0] for i in f.readlines()]

    for i, row in request_df.iterrows():
        #offer_path = os.path.join(travel_offers_path, offer + '.csv')
        #t_off = pd.read_csv(offer_path)
        #print(t_off.head())

        t_off = row.to_frame().transpose()
        t_off.index = range(len(t_off))
        print(t_off.head())

        new_t_off, new_columns = preprocessing_user_profile(t_off, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_t_off[new_t_off.columns.intersection(all_class_cols)]

        columns_n = list(classifier_df.columns.values)
        classifier_np = classifier_df.values
        # min_max_scaler = preprocessing.MinMaxScaler()
        # classifier_np_scaled = min_max_scaler.fit_transform(classifier_np)
        classifier_np_scaled = classifier_np
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns_n, index=range(len(classifier_df)))

        common_cols = list(set(classifier_df.columns.values).intersection(set(classifier_model_col)))
        uncommon_cols = list(set(classifier_model_col) - set(classifier_df.columns.values))

        classifier_df2 = pd.DataFrame(columns=classifier_model_col, index=range(len(classifier_df)))
        for offer_id in range(len(classifier_df)):
            classifier_df2.loc[offer_id, common_cols] = classifier_df.loc[offer_id, common_cols]
            classifier_df2.loc[offer_id, uncommon_cols] = 0

        classifier_df2 = classifier_df2.fillna(0)
        pred = classifier_model.predict(classifier_df2)

        class_prob = classifier_model.predict_proba(classifier_df2)
        if pred == 1:
            buying_prob.append([t_off['Travel Offer ID'].values[0], class_prob[0, 1]])
        else:
            not_buying_prob.append([t_off['Travel Offer ID'].values[0], class_prob[0, 0]])

    buying_prob = sorted(buying_prob, key=lambda x: x[1], reverse=True)
    not_buying_prob = sorted(not_buying_prob, key=lambda x: x[1])

    sorted_offers_scores = buying_prob + not_buying_prob
    # print(sorted_offers_scores)
    sorted_travel_offers = [i[0] for i in sorted_offers_scores]

    """
    with open(os.path.join(os.path.dirname(travel_offers[0]), 'sorted_offers.txt'), 'w') as f:
        for to in sorted_travel_offers:
            print(to)
            f.write(to+'\n')
    """
    return sorted_travel_offers
