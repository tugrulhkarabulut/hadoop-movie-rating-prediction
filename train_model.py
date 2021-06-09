from random import random
import pandas as pd
import pickle
import argparse
from sklearn.model_selection import train_test_split
from pydoop import hdfs
from sklearn.ensemble import RandomForestClassifier

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)

    args = parser.parse_args()

    return args

def train_model(data_input, model_output):

    with hdfs.open(data_input) as f:
        df = pd.read_csv(f)

    X, y = df.drop(columns=['rating', 'review_id']).values, df['rating'].values
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=1)

    model = RandomForestClassifier(n_estimators=600, max_depth=12, class_weight='balanced_subsample')
    model.fit(X_train, y_train)
    score_train = model.score(X_train, y_train)
    score_test = model.score(X_test, y_test)
    
    print('Train accuracy: {}%'.format(score_train * 100))
    print('Test accuracy: {}%'.format(score_test * 100))

    with open(model_output, 'wb') as f:
        pickle.dump(model, f, pickle.HIGHEST_PROTOCOL)


    return score_train * 100, score_test * 100

if __name__ == '__main__':
    args = parse_arguments()

    train_model(args.input, args.output)    