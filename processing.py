import pandas as pd
from tqdm import tqdm
import contractions
import string
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
import re

nltk.download('stopwords')
stop_words = stopwords.words('english')
tqdm.pandas()

DATA_PATH = '~/Documents/vm_data/'
FILE_NAME = 'part-01.json'

def preprocess_text(df):
    stemmer = PorterStemmer()

    df = df.str.lower()
    df = df.apply(contractions.fix)
    #df = df.apply(lambda s: "".join([ch for ch in s if ch not in string.punctuation and ch not in string.digits and ch in string.printable]))
    df = df.apply(lambda s: re.sub('[^a-zA-Z ]+', '', s))
    df = df.apply(lambda s: " ".join([stemmer.stem(word) for word in s.split() if word not in stop_words]))
    
    return df

def preprocess_data(df):
    df['review_summary'] = preprocess_text(df['review_summary'])
    df['review_detail'] = preprocess_text(df['review_detail'])

    return df

def extract_helpful_count(df):
    helpful = df['helpful'].apply(lambda h: h[0]).apply(lambda h: h.replace(',', '')).astype('int')
    all_ = df['helpful'].apply(lambda h: h[1]).apply(lambda h: h.replace(',', '')).astype('int')

    df['helpful_ratio'] = helpful / all_

    return df

def write_list_to_txt(list_, output_file='output.txt'):
    with open(output_file, 'w') as f:
        for el in tqdm(list_):
            f.write("{}\n".format(el))

if __name__ == '__main__':
    df = pd.read_json(DATA_PATH + FILE_NAME)
    df = df.dropna()
    df = preprocess_data(df)
    df = extract_helpful_count(df)

    df = df.dropna()
    del df['helpful']
    del df['reviewer']
    del df['review_date']

    df.to_csv('preprocessed_part1.csv', index=False)
