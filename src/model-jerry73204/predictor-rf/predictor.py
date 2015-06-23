#!/usr/bin/env python3
from sklearn.ensemble import RandomForestClassifier
import sys
import numpy as np

FEATURE_POLYLINE_SIZE = 400

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: %s train_features train_labels test_features' % sys.argv[0])
        exit(1)

    dtype_feature_vector = np.dtype('f8, f8, ' * FEATURE_POLYLINE_SIZE + 'i, i, i, i')
    dtype_label = np.dtype('i')

    features_train = np.memmap(sys.argv[1], dtype_feature_vector, mode='r')
    labels_train = np.memmap(sys.argv[2], dtype_label, mode='r')
    features_test = np.memmap(sys.argv[3], dtype_feature_vector, mode='r')

    classifier = RandomForestClassifier(n_estimators=500)
    classifier.fit(features_train, labels_train)
    predictions = classifier.predict(features_test)

    print(predictions)
