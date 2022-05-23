import sys
from sklearn.metrics import confusion_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
import numpy as np
import os
import pickle
sys.path.append('../')


def compute_accuracy_metrics(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred, labels=[1, 0]).ravel()
    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    fscore = (2 * precision * recall) / (precision + recall)
    fpr = fp / (fp + tn)
    fnr = fn / (fn + tp)
    return accuracy, precision, recall, fscore, fpr, fnr


def classifiers_grid_search_all(classifier_df2, target_col='Bought Tag'):
    classifier_df2 = classifier_df2.reindex(sorted(classifier_df2.columns), axis=1)
    classifier_df2 = classifier_df2.fillna(0)
    target = classifier_df2[target_col]
    features = classifier_df2.drop(target_col, axis=1)

    x_train, x_test, y_train, y_test = train_test_split(features, target, train_size=0.8, random_state=2021)
    svm_parameters = {'kernel': ('linear', 'poly', 'rbf'), 'C': [1, 10], 'probability': [True]}

    dt_parameters = {'criterion': ['entropy'], 'max_depth': [5, 7, 10], 'min_samples_leaf': [1, 2, 4],
                     'max_features': [1, 5, 10, 20, 30, 40], 'max_leaf_nodes': [2, 5, 10]}
    lr_parameters = {'penalty': ['l1', 'l2', 'elasticnet', 'none'],
                     'solver': ['lbfgs', 'liblinear'], 'fit_intercept': [True, False],
                     'C': [0.5, 1]}
    rf_parameters = {'n_estimators': [100], 'criterion': ['entropy'], 'max_depth': [5, 7, 10],
                     'max_features': [1, 5, 10, 20, 30, 40], 'bootstrap': [True]}

    knn_parameters = {'n_neighbors': [1, 3, 5, 10], 'weights': ['uniform', 'distance'], 'p': range(1, 6)}


    print('gridsearch - svc')
    svc = SVC(probability=True)
    clf = GridSearchCV(svc, svm_parameters, cv=5)
    clf.fit(x_train, y_train)
    best_svc = SVC(**clf.best_params_).fit(x_train, y_train)

    print('gridsearch - decision_tree')
    decision_tree = DecisionTreeClassifier()
    dt_gs = GridSearchCV(decision_tree, dt_parameters, cv=5)
    dt_gs.fit(x_train, y_train)
    best_dt = DecisionTreeClassifier(**dt_gs.best_params_).fit(x_train, y_train)

    print('gridsearch - LogisticRegression')
    lr = LogisticRegression()
    lr_gs = GridSearchCV(lr, lr_parameters, cv=5)
    lr_gs.fit(x_train, y_train)
    best_lr = LogisticRegression(**lr_gs.best_params_).fit(x_train, y_train)

    print('gridsearch - RandomForestClassifier')
    rf = RandomForestClassifier()
    rf_gs = GridSearchCV(rf, rf_parameters, cv=5)
    rf_gs.fit(x_train, y_train)
    best_rf = RandomForestClassifier(**rf_gs.best_params_).fit(x_train, y_train)

    print('gridsearch - knn')
    knn = KNeighborsClassifier()
    knn_gs = GridSearchCV(knn, knn_parameters, cv=5)
    knn_gs.fit(x_train, y_train)
    best_knn = KNeighborsClassifier(**knn_gs.best_params_).fit(x_train, y_train)

    best_classifiers = [best_svc, best_dt, best_lr, best_rf, best_knn]
    
    test_results = []
    for cla in best_classifiers:
        y_pred = cla.predict(x_test)
        accuracy_te, precision_te, recall_te, fscore_te, fpr_te, fnr_te = compute_accuracy_metrics(y_test, y_pred)
        test_results.append(fscore_te)

    best_cla_id = int(np.argmax(test_results))
    best_classifier = best_classifiers[best_cla_id]

    col_list = list(x_test.columns.values)
    return best_classifier, col_list
