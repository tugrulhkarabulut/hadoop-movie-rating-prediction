import argparse
import pandas as pd
from pydoop import hdfs

from word_counter import WordCounter
from inverse_document_frequency_calculator import InverseDocumentFrequencyCalculator
from term_frequency_calculator import TermFrequencyCalculator
from data_builder import DataBuilder


def extract_features(input_paths, output_path, max_words_summary=50, max_words_review=100, env='hadoop'):
    input_paths_str = ''
    if isinstance(input_paths, list):
        input_paths_str = ','.join(input_paths_str)
    else:
        input_paths_str = input_paths


    w = WordCounter(args=[input_paths_str, '-r', env, '--column_index', '3'])
    summary_word_counts = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            summary_word_counts[key] = value


    w = WordCounter(args=[input_paths_str, '-r', env, '--column_index', '5'])
    review_word_counts = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            review_word_counts[key] = value

    n_rows = InverseDocumentFrequencyCalculator.calc_n_rows(input_paths_str)
    w = InverseDocumentFrequencyCalculator(args=[input_paths_str, '-r', env, '--n_rows', str(n_rows), '--column_index', '5'])
    word_summary_idfs = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            word_summary_idfs[key] = value

    w = InverseDocumentFrequencyCalculator(args=[input_paths_str, '-r', env, '--n_rows', str(n_rows), '--column_index', '5'])
    word_review_idfs = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            word_review_idfs[key] = value


    summary_word_counts = pd.DataFrame({'word': summary_word_counts.keys(), 'count': summary_word_counts.values()})
    review_word_counts = pd.DataFrame({'word': review_word_counts.keys(), 'count': review_word_counts.values()})

    word_summary_idfs = pd.DataFrame({'word': word_summary_idfs.keys(), 'idf': word_summary_idfs.values()})
    word_review_idfs = pd.DataFrame({'word': word_review_idfs.keys(), 'idf': word_review_idfs.values()})


    words_summary = list(summary_word_counts.sort_values(by='count', ascending=False).iloc[:max_words_summary].reset_index(drop=True)['word'])
    words_summary_str = ','.join(words_summary)

    words_review = list(review_word_counts.sort_values(by='count', ascending=False).iloc[:max_words_review].reset_index(drop=True)['word'])
    words_review_str = ','.join(words_review)


    w = TermFrequencyCalculator(args=[input_paths_str, '-r', env, '--column_index', '3', '--words', words_summary_str])
    word_doc_summary_tfs = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            word_doc_summary_tfs[tuple(key)] = value

    w = TermFrequencyCalculator(args=[input_paths_str, '-r', env, '--column_index', '5', '--words', words_review_str])
    word_doc_review_tfs = {}
    with w.make_runner() as runner:
        runner.run()
        for key, value in w.parse_output(runner.cat_output()):
            word_doc_review_tfs[tuple(key)] = value


    summary_tf_keys = word_doc_summary_tfs.keys()
    summary_tf_review_ids = [el[0] for el in summary_tf_keys]
    summary_tf_words = [el[1] for el in summary_tf_keys]
    
    review_tf_keys = word_doc_review_tfs.keys()
    review_tf_review_ids = [el[0] for el in review_tf_keys]
    review_tf_words = [el[1] for el in review_tf_keys]

    word_doc_summary_tfs = pd.DataFrame({'review_id': summary_tf_review_ids,  'word': summary_tf_words, 'tf': word_doc_summary_tfs.values()})
    word_doc_review_tfs = pd.DataFrame({'review_id': review_tf_review_ids,'word': review_tf_words, 'tf': word_doc_review_tfs.values()})

    result = word_doc_summary_tfs.merge(word_summary_idfs, on='word')
    result['tf_idf'] = result['tf'] * result['idf']
    tf_idf_summary = pd.DataFrame()
    tf_idf_summary['review_id'] = result['review_id']
    tf_idf_summary['word'] = result['word']
    tf_idf_summary['tf_idf'] = result['tf_idf']


    result = word_doc_review_tfs.merge(word_review_idfs, on='word')
    result['tf_idf'] = result['tf'] * result['idf']
    tf_idf_review = pd.DataFrame()
    tf_idf_review['review_id'] = result['review_id']
    tf_idf_review['word'] = result['word']
    tf_idf_review['tf_idf'] = result['tf_idf']


    builder = DataBuilder()
    builder.data = pd.read_csv(input_paths_str)
    builder.tf_idf_data = [tf_idf_summary, tf_idf_review]
    builder.build()
    builder.save(output_path)

    
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)
    parser.add_argument('--max_words_summary', type=int, default=50)
    parser.add_argument('--max_words_review', type=int, default=100)

    args = parser.parse_args()

    return args

if __name__ == '__main__':

    args = parse_arguments()
    with hdfs.open(args.input) as f:
        df = pd.read_csv(f)

    print('done')
    
    #extract_features(args.input, args.output, args.max_words_summary, args.max_words_review, env='hadoop')