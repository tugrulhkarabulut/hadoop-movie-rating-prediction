import numpy as np
import pandas as pd

def tf_idf(term_frequencies_path, document_counts_path, output_path):
    tf_s = pd.read_csv(term_frequencies_path, sep='\t', header=None)
    review_id = tf_s[0].apply(lambda x: x.split(', ')[0][2:-1])
    word = tf_s[0].apply(lambda x: x.split(', ')[1][1:-2])
    del tf_s[0]
    tf_s['review_id'] = review_id
    tf_s['word'] = word
    tf_s['tf'] = tf_s[1]
    del tf_s[1]

    idf_s = pd.read_csv(document_counts_path, sep='\t', header=None)
    idf_s.columns = ['word', 'idf']
    

    result = tf_s.merge(idf_s, on='word')

    result['tf_idf'] = result['tf'] * result['idf']

    df = pd.DataFrame()
    df['review_id'] = result['review_id']
    df['word'] = result['word']
    df['tf_idf'] = result['tf_idf']
    df.to_csv(output_path, index=False)

if __name__ == '__main__':
    tf_idf('review_detail_term_frequencies.txt', 'review_detail_document_counts.txt', 'tf_idf_review.csv')
    tf_idf('review_summary_term_frequencies.txt', 'review_summary_document_counts.txt', 'tf_idf_summary.csv')