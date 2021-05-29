import pickle
import pandas as pd
from random_forest_classifier import RandomForestClassifier

if __name__ == '__main__':
    with open('model.pickle', 'rb') as f:
        model = pickle.load(f)

    df = pd.read_csv('test_data.csv')
    del df['movie']
    X_test, y_test = df.drop(columns=['rating']).values, df['rating'].values
    
    print(model.score(X_test, y_test))
    