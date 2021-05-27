import pandas as pd
from decision_tree import DecisionTreeClassifier
from random_forest_classifier import RandomForestClassifier
import pickle

if __name__ == '__main__':
    df = pd.read_csv('train_data.csv')
    #df = df.iloc[:10000]
    del df['movie']
    X_train, y_train = df.drop(columns=['rating']).values, df['rating'].values

    #model = DecisionTreeClassifier(split_method='binary', max_depth=12)
    model = RandomForestClassifier(split_method='binary', max_depth=12, n_trees=10)
    model.fit(X_train, y_train)
    score = model.score(X_train, y_train)
    

    with open('model.pickle', 'wb') as f:
        pickle.dump(model, f, pickle.HIGHEST_PROTOCOL)

    print(score)