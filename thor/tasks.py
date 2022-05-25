from sklearn import preprocessing
from data_preprocessing import *
from classifiers import *


# task 1
def make_classifier_for_all_users(user_dataframes, classifier_columns, target_column, one_hot_col_cat, one_hot_col_cat_list):

    user_dataframes_for_classifier = []
    for user_data in user_dataframes:
        new_user_data, new_columns = preprocessing_user_profile(user_data, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_user_data[new_user_data.columns.intersection(all_class_cols)]
        classifier_df = classifier_df.drop(['Driving speed', 'Cycling speed'], axis=1)

        columns = list(classifier_df.columns.values)
        classifier_np = classifier_df.values
        min_max_scaler = preprocessing.MinMaxScaler()
        classifier_np_scaled = min_max_scaler.fit_transform(classifier_np)
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns, index=range(len(classifier_df)))

        nunique = classifier_df.nunique()
        cols_to_drop = nunique[nunique == 1].index
        classifier_df2 = classifier_df.drop(cols_to_drop, axis=1)
        user_dataframes_for_classifier.append(classifier_df2)

    df_all_users = pd.concat(user_dataframes_for_classifier, ignore_index=True)
    df_all_users = df_all_users.fillna(0)
    classifier = classifiers_grid_search_all(df_all_users, target_col=target_column)
    return classifier


# task 4
def sort_offers(request_df, classifier_model, classifier_model_col, one_hot_col_cat, one_hot_col_cat_list, classifier_columns):
    request_df = request_df.reindex(sorted(request_df.columns), axis=1)
    offer_scores = {}

    for i, row in request_df.iterrows():

        t_off = row.to_frame().transpose()
        t_off.index = range(len(t_off))

        new_t_off, new_columns = preprocessing_user_profile(t_off, one_hot_col_cat, one_hot_col_cat_list)
        all_class_cols = classifier_columns + new_columns

        classifier_df = new_t_off[new_t_off.columns.intersection(all_class_cols)]

        columns_n = list(classifier_df.columns.values)
        classifier_np = classifier_df.values
        classifier_np_scaled = classifier_np
        classifier_df = pd.DataFrame(data=classifier_np_scaled, columns=columns_n, index=range(len(classifier_df)))

        common_cols = list(set(classifier_df.columns.values).intersection(set(classifier_model_col)))
        uncommon_cols = list(set(classifier_model_col) - set(classifier_df.columns.values))

        classifier_df2 = pd.DataFrame(columns=classifier_model_col, index=range(len(classifier_df)))
        for offer_id in range(len(classifier_df)):
            classifier_df2.loc[offer_id, common_cols] = classifier_df.loc[offer_id, common_cols]
            classifier_df2.loc[offer_id, uncommon_cols] = 0

        classifier_df2 = classifier_df2.fillna(0)

        buy_proba = classifier_model.predict_proba(classifier_df2)[0, 1]
        offer_scores[row['Travel Offer ID']] = buy_proba

    sorted_offers = {offer_id: offer_scores[offer_id] for offer_id in sorted(offer_scores, key=offer_scores.get, reverse=True)}
    return sorted_offers
