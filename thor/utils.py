# -*- coding: utf-8 -*-
import pathlib

def init(config):

    def split_list(alist, separator=','):
        return [i.strip() for i in alist.split(separator)]

    global execution_mode
    execution_mode = config['running']['mode']

    filename = pathlib.Path(__file__).name
    print(f"execution_mode ({filename}): {execution_mode}")


def read_offers_prod(offer_id):
    return offer_id


def read_user_data_prod(user_id):
    return user_id


def write_cluster(cluster_model, cluster_model_col):
    print("clusters_path:", clusters_path)
    cluster_model = pickle.load(open(os.path.join(clusters_path, 'cluster.pkl'), 'rb'))
    with open(os.path.join(clusters_path, 'cluster.txt'), 'rb') as f:
        cluster_model_col = [i.decode("utf-8","replace").split('\n')[0] for i in f.readlines()]


def open_user_model(label):
    user_classifier = pickle.load(open(os.path.join(clusters_path, 'cluster_' + str(label) + '_classifier.pkl'), 'rb'))
    with open(os.path.join(clusters_path, 'cluster_' + str(label) + '_columns.txt'), 'rb') as f:
        user_classifier_col = [i.decode("utf-8","replace").split('\n')[0] for i in f.readlines()]

    return user_classifier, user_classifier_col


def get_user_model_cache(user):
    print("open classifier:", user)
    classifier_model, classifier_model_col = None, None

    pickled_model = obj_cache.get(f'{user}:model')
    pickled_model_col = obj_cache.get(f'{user}:model')
    

    if pickled_model:
        classifier_model = pickled_model.loads()
        classifier_model_col = pickled_model_col.loads()

    return classifier_model, classifier_model_col


def get_user_model_file(user):
    print("open classifier:", os.path.join(classifiers_path, user + '_classifier.pkl'))
    classifier_model, classifier_model_col = None, None

    if pathlib.Path(classifiers_path, user+'_classifier.pkl').exists():
        classifier_model = pickle.load(open(os.path.join(classifiers_path, user + '_classifier.pkl'), 'rb'))
        with open(os.path.join(classifiers_path, user + '_columns.txt'), 'r') as f:
            classifier_model_col = [i.split('\n')[0] for i in f.readlines()]
    # classifier_model = sklearn.tree.DecisionTreeClassifier(criterion='entropy',
    #                                                        max_depth=5,
    #                                                        max_features=5,
    #                                                        max_leaf_nodes=10,
    #                                                        min_samples_leaf=4,
    #                                                        splitter='random')
    # with open(os.path.join(classifiers_path, 'columns.txt'), 'r') as f:
    #     classifier_model_col = [i.split('\n')[0] for i in f.readlines()]


    return classifier_model, classifier_model_col
