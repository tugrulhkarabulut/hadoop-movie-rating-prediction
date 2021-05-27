import numpy as np
import pandas as pd
import uuid
from decision_tree import DecisionTreeClassifier
from bagging_predictor import BaggingPredictor
import os

class RandomForestClassifier:
    def __init__(self, 
                 n_trees=100, 
                 tol=0.1, 
                 max_depth=None, 
                 min_members=10, 
                 criterion='entropy', 
                 split_method='nary', 
                 max_features=None):
        self.n_trees = n_trees
        self.tol = tol
        self.max_depth = max_depth
        self.min_members = min_members
        self.criterion = criterion
        self.split_method = split_method
        self.max_features = max_features

    def fit(self, X, y):
        self.classifiers_ = []
        X_ = self.__get_values(X)
        y_ = self.__get_values(y)
        for _ in range(self.n_trees):
            sample = self.__get_sample(X.shape[0])
            model = DecisionTreeClassifier(
                self.tol, 
                self.max_depth, 
                self.min_members, 
                self.criterion, 
                self.split_method, 
                self.max_features
            )
            model.fit(X_[sample], y_[sample])
            self.classifiers_.append(model)

    def predict(self, X):        
        trees = np.arange(0, self.n_trees)
        process_data_id = str(uuid.uuid4())
        process_data_path = 'rf_' + process_data_id + '.txt'
        process_output_path = 'result_' + process_data_id + '.txt'

        
        with open(process_data_path, 'w') as f:
            for t in trees:
                f.write(str(t) + '\n')

        BaggingPredictor.set_input_path(process_data_path)
        BaggingPredictor.set_output_path(process_output_path)
        BaggingPredictor.set_classifiers(self.classifiers_)
        BaggingPredictor.set_calc_function(lambda clf: clf.predict(X))
        BaggingPredictor.run()

        os.remove(process_data_path)

        preds = pd.read_csv(process_output_path, sep='\t', header=None)
        preds[0] = preds[0].astype('int')
        preds = preds.sort_values(by=0)

        os.remove(process_output_path)

        return preds[1].values
    
    def score(self, X, y):
        pred = self.predict(X)
        return pred[y == pred].size / pred.size

    def __get_sample(self, sample_size):
        return np.random.choice(sample_size, size=sample_size)
    
    def __get_values(self, data):
        if isinstance(data, pd.DataFrame) or isinstance(data, pd.Series):
            return data.values
        return data