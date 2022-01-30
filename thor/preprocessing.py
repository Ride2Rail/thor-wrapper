import pandas as pd
from pandas.api.types import CategoricalDtype


def make_one_hot_cat_columns(user_data, one_hot_col_cat):
    created_cols = []
    for col in one_hot_col_cat:
        vals = list(set(user_data[col]))
        user_data[col] = user_data[col].astype(CategoricalDtype(vals))
        created_cols += [col+'_'+i for i in vals]
        user_data = pd.concat([user_data, pd.get_dummies(user_data[col], prefix=col)], axis=1)
        user_data.drop([col], axis=1, inplace=True)

    return user_data, created_cols


def make_one_hot_catlist_columns(user_data, one_hot_col_cat_list):
    created_cols = []
    for col in one_hot_col_cat_list:
        vals = list(set(user_data[col]))
        new_vals = []
        for val in vals:
            tt = [ii for ii in val.split("'") if '[' not in ii and ']' not in ii and ',' not in ii and len(ii) > 1]
            new_vals += tt

        new_cols = [col+'_'+nv for nv in list(set(new_vals))]
        created_cols += new_cols
        new_df = pd.DataFrame(columns=new_cols, index=range(len(user_data)))
        for ind in range(len(user_data)):
            this_vals = user_data.loc[ind, col]
            this_vals_ind = [ii for ii in this_vals.split("'") if '[' not in ii and ']' not in ii and ',' not in ii and
                             len(ii) > 1]
            eee = [1 if i.split('_')[1] in this_vals_ind else 0 for i in new_cols]
            new_df.loc[ind, :] = eee

        user_data = pd.concat([user_data, new_df], axis=1)
        user_data.drop([col], axis=1, inplace=True)
    return user_data, created_cols


def preprocessing_user_profile(user_data, one_hot_col_cat, one_hot_col_cat_list):

    user_data_new, created_cols = make_one_hot_cat_columns(user_data, one_hot_col_cat)

    user_data_new, created_cols2 = make_one_hot_catlist_columns(user_data_new, one_hot_col_cat_list)

    if 'Walking distance to stop' in list(user_data_new.columns.values):
        user_data_new['Walking distance to stop'] = [int(str(i).split('m')[0]) for i in
                                                     list(user_data_new['Walking distance to stop'])]
    if 'Cycling distance to stop' in list(user_data_new.columns.values):
        user_data_new['Cycling distance to stop'] = [int(str(i).split('m')[0]) for i in
                                                     list(user_data_new['Cycling distance to stop'])]
    if 'Date Of Birth' in list(user_data_new.columns.values):
        try:
            user_data_new['Date Of Birth'] = [int(str(i).split('/')[2]) for i in list(user_data_new['Date Of Birth'])]

        except:
            user_data_new['Date Of Birth'] = [int(str(i).split('-')[2]) for i in list(user_data_new['Date Of Birth'])]
    return user_data_new, created_cols + created_cols2
