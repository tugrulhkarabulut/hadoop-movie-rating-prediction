from flask import Blueprint
from flask import request
import os
import pandas as pd
import re
import numpy as np

from ..processing import preprocess_single

bp = Blueprint('predict', __name__)


def tf_idf(words, idf):
    word_counts = {}
    for word in words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1

    tf_idfs = []
    for word in idf.index:
        if word in word_counts:
            tf_idfs.append(word_counts[word] / len(words) * idf.iloc[word]['idf'])
        else:
            tf_idfs.append(0)

    return tf_idfs

@bp.route('/predict', methods=['POST'])
def build():
    req_data = request.get_json()
    process_name = req_data['model']
    process_path = './app_data/' + process_name

    feature_types = []
    with open(process_path + '/feature_types.txt') as f:
        for line in f:
            feature_types.append(line[:-1])



    predict_input = []
    summary = preprocess_single(req_data['summary_input']).strip()
    review = preprocess_single(req_data['review_input']).strip()
    cleaned_summary = re.sub('[^a-zA-Z ]+', '', summary)
    cleaned_review = re.sub('[^a-zA-Z ]+', '', review)
    spoiler = int(req_data['spoiler'])
    helpful_count = int(req_data['helpful_count'])
    unhelpful_count = int(req_data['unhelpful_count'])
    helpful_ratio = helpful_count / (helpful_count + unhelpful_count)

    predict_input.append(spoiler)
    predict_input.append(helpful_ratio)

    if 'tf_idf' in feature_types:
        idf_summary = pd.read_csv(process_path + '/idf_summary.csv')
        idf_review = pd.read_csv(process_path + '/idf_review.csv')
        idf_summary.index = idf_summary['word']
        idf_review.index = idf_review['word']
        summary_words = cleaned_summary.split()
        review_words = cleaned_review.split()

        summary_words = [word for word in summary_words if word in idf_summary.index]
        review_words = [word for word in review_words if word in idf_review.index]

        tf_idf_summary = tf_idf(summary_words, idf_summary)
        tf_idf_review = tf_idf(review_words, idf_review)

        predict_input  = predict_input + tf_idf_summary + tf_idf_review


    if 'exclamation' in feature_types:
        regex = r'[\?\!]'
        sum_matches = re.findall(regex, summary)

        exc_count_summary = 0
        q_count_summary = 0

        for m in sum_matches:
            if m == '!':
                exc_count_summary += 1
            else:
                q_count_summary += 1

        rev_matches = re.findall(regex, review)

        exc_count_review = 0
        q_count_review = 0

        for m in rev_matches:
            if m == '!':
                exc_count_review += 1
            else:
                q_count_review += 1

        predict_input = predict_input + [exc_count_summary, q_count_summary, exc_count_review, q_count_review]


    if 'n_gram_count' in feature_types:
        summary_n_grams = pd.read_csv(process_path + '/summary_n_grams.csv')['word']
        review_n_grams = pd.read_csv(process_path + '/review_n_grams.csv')['word']

        summary_n_grams_existences = []
        for n_gram in summary_n_grams:
                n_gram_spaced = ' '.join(n_gram.split('_'))
                res = re.search(n_gram_spaced, cleaned_summary)
                exists = 1 if res else 0
                summary_n_grams_existences.append(exists)

        review_n_grams_existences = []
        for n_gram in review_n_grams:
                n_gram_spaced = ' '.join(n_gram.split('_'))
                res = re.search(n_gram_spaced, cleaned_review)
                exists = 1 if res else 0
                review_n_grams_existences.append(exists)

        predict_input = predict_input + summary_n_grams_existences + review_n_grams_existences


    print(predict_input)

    return {}


@bp.route('/get-models', methods=['GET'])
def get_models():
    models = []
    model_names = os.listdir('./app_data')

    for m in model_names:
        if os.listdir('./app_data/' + m):
            models.append(m)
    

    res = {}
    res['models'] = models

    return res