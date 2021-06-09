import argparse
import pandas as pd
from pydoop import hdfs

from .word_counter import WordCounter
from .inverse_document_frequency_calculator import InverseDocumentFrequencyCalculator
from .term_frequency_calculator import TermFrequencyCalculator
from .exlamation_mark_counter import ExclamationMarkCounter
from .n_gram_counter import NGramCounter
from .n_gram_local_counter import NGramLocalCounter
from .data_builder import DataBuilder

def extract_features(process_path, input_paths, hadoop_output, feature_types=['tf_idf', 'exclamation', 'n_gram_count'], max_words_summary=10, max_words_review=20, env='hadoop'):
    input_paths_str = ''
    if isinstance(input_paths, list):
        input_paths_str = ','.join(input_paths_str)
    else:
        input_paths_str = input_paths


    feature_data = []
    if 'tf_idf' in feature_types:
        w = WordCounter(args=[input_paths_str, '-r', env, '--column_index', '8'])
        summary_word_counts = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                summary_word_counts[key] = value


        w = WordCounter(args=[input_paths_str, '-r', env, '--column_index', '9'])
        review_word_counts = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                review_word_counts[key] = value


        summary_word_counts = pd.DataFrame({'word': summary_word_counts.keys(), 'count': summary_word_counts.values()})
        review_word_counts = pd.DataFrame({'word': review_word_counts.keys(), 'count': review_word_counts.values()})

        words_summary = list(summary_word_counts.sort_values(by='count', ascending=False).iloc[:max_words_summary].reset_index(drop=True)['word'])
        words_summary_str = ','.join(words_summary)

        words_review = list(review_word_counts.sort_values(by='count', ascending=False).iloc[:max_words_review].reset_index(drop=True)['word'])
        words_review_str = ','.join(words_review)


        with hdfs.open(input_paths_str) as f:
            n_rows = InverseDocumentFrequencyCalculator.calc_n_rows(f)

        w = InverseDocumentFrequencyCalculator(args=[input_paths_str, '-r', env, '--n_rows', str(n_rows), '--column_index', '8', '--words', words_summary_str])
        word_summary_idfs = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                word_summary_idfs[key] = value

        w = InverseDocumentFrequencyCalculator(args=[input_paths_str, '-r', env, '--n_rows', str(n_rows), '--column_index', '9', '--words', words_review_str])
        word_review_idfs = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                word_review_idfs[key] = value

        word_summary_idfs = pd.DataFrame({'word': word_summary_idfs.keys(), 'idf': word_summary_idfs.values()})
        word_review_idfs = pd.DataFrame({'word': word_review_idfs.keys(), 'idf': word_review_idfs.values()})


        word_summary_idfs_subset = word_summary_idfs[word_summary_idfs['word'].isin(words_summary)]
        word_review_idfs_subset = word_review_idfs[word_review_idfs['word'].isin(words_review)]
        word_summary_idfs_subset.to_csv(process_path + '/idf_summary.csv', index=False)
        word_review_idfs_subset.to_csv(process_path + '/idf_review.csv', index=False)


        w = TermFrequencyCalculator(args=[input_paths_str, '-r', env, '--column_index', '8', '--words', words_summary_str])
        word_doc_summary_tfs = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                word_doc_summary_tfs[tuple(key)] = value

        w = TermFrequencyCalculator(args=[input_paths_str, '-r', env, '--column_index', '9', '--words', words_review_str])
        word_doc_review_tfs = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                word_doc_review_tfs[tuple(key)] = value


        print('Jobs are done!')

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

        print('tf idf calculated')
        feature_data.append(tf_idf_summary)
        feature_data.append(tf_idf_review)


    if 'exclamation' in feature_types:
        w = ExclamationMarkCounter(args=[input_paths_str, '-r', env, '--column_index', '7'])
        exclamation_mark_counts_summary = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                exclamation_mark_counts_summary[tuple(key)] = value

        w = ExclamationMarkCounter(args=[input_paths_str, '-r', env, '--column_index', '8'])
        exclamation_mark_counts_review = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                exclamation_mark_counts_review[tuple(key)] = value


        summary_exc_keys = exclamation_mark_counts_summary.keys()
        summary_exc_review_ids = [el[0] for el in summary_exc_keys]
        summary_excs = [el[1] for el in summary_exc_keys]

        review_exc_keys = exclamation_mark_counts_review.keys()
        review_exc_review_ids = [el[0] for el in review_exc_keys]
        review_excs = [el[1] for el in exclamation_mark_counts_review]

        exc_doc_summary_counts = pd.DataFrame({'review_id': summary_exc_review_ids,  'exc': summary_excs, 'count': exclamation_mark_counts_summary.values()})
        exc_doc_review_counts = pd.DataFrame({'review_id': review_exc_review_ids, 'exc': review_excs, 'count': exclamation_mark_counts_review.values()})

        feature_data.append(exc_doc_summary_counts)
        feature_data.append(exc_doc_review_counts)

    if 'n_gram_count' in feature_types:
        w = NGramCounter(args=[input_paths_str, '-r', env, '--column_index', '8'])
        n_gram_counts_summary = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                n_gram_counts_summary[key] = value

        w = NGramCounter(args=[input_paths_str, '-r', env, '--column_index', '9'])
        n_gram_counts_review = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                n_gram_counts_review[key] = value


        summary_ngram_counts = pd.DataFrame({'word': n_gram_counts_summary.keys(), 'count': n_gram_counts_summary.values()})
        review_ngram_counts = pd.DataFrame({'word': n_gram_counts_review.keys(), 'count': n_gram_counts_review.values()})

        n_gram_summary = list(summary_ngram_counts.sort_values(by='count', ascending=False).iloc[:5].reset_index(drop=True)['word'])
        n_gram_summary_str = ','.join(n_gram_summary)

        n_gram_review = list(review_ngram_counts.sort_values(by='count', ascending=False).iloc[:15].reset_index(drop=True)['word'])
        n_gram_review_str = ','.join(n_gram_review)


        summary_ngram_counts_subset = summary_ngram_counts[summary_ngram_counts['word'].isin(n_gram_summary)][['word']]
        review_ngram_counts_subset = review_ngram_counts[review_ngram_counts['word'].isin(n_gram_review)][['word']]
        summary_ngram_counts_subset.to_csv(process_path + '/summary_n_grams.csv', index=False)
        review_ngram_counts_subset.to_csv(process_path + '/review_n_grams.csv', index=False)

        w = NGramLocalCounter(args=[input_paths_str, '-r', env, '--column_index', '8', '--n_grams', n_gram_summary_str])
        n_gram_counts_summary = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                n_gram_counts_summary[tuple(key)] = value

        w = NGramLocalCounter(args=[input_paths_str, '-r', env, '--column_index', '9', '--n_grams', n_gram_review_str])
        n_gram_counts_review = {}
        with w.make_runner() as runner:
            runner.run()
            for key, value in w.parse_output(runner.cat_output()):
                n_gram_counts_review[tuple(key)] = value


        summary_n_gram_keys = n_gram_counts_summary.keys()
        summary_n_gram_review_ids = [el[0] for el in summary_n_gram_keys]
        summary_ngrams = [el[1] for el in summary_n_gram_keys]

        review_n_gram_keys = n_gram_counts_review.keys()
        review_n_gram_review_ids = [el[0] for el in review_n_gram_keys]
        review_ngrams = [el[1] for el in review_n_gram_keys]

        summary_n_gram_counts = pd.DataFrame({'review_id': summary_n_gram_review_ids,  'n_gram': summary_ngrams, 'count': n_gram_counts_summary.values()})
        review_n_gram_counts = pd.DataFrame({'review_id': review_n_gram_review_ids, 'n_gram': review_ngrams, 'count': n_gram_counts_review.values()})


        feature_data.append(summary_n_gram_counts)
        feature_data.append(review_n_gram_counts)

    builder = DataBuilder()

    if env == 'hadoop':
        with hdfs.open(input_paths_str) as f:
            builder.data = pd.read_csv(f, usecols=['review_id', 'rating', 'spoiler_tag', 'helpful_ratio'])
    else:
        builder.data = pd.read_csv(input_paths_str, usecols=['review_id', 'rating', 'spoiler_tag', 'helpful_ratio'])
    
    print('opened file from hadoop')

    builder.tf_idf_data = feature_data
    builder.build()

    print('train data is built')

    builder.save(process_path + '/output.csv')

    if env == 'hadoop':
        hdfs.put(process_path + '/output.csv', hadoop_output)

    
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)
    parser.add_argument('--hadoop_output', type=str, required=True)
    parser.add_argument('--max_words_summary', type=int, default=10)
    parser.add_argument('--max_words_review', type=int, default=20)

    args = parser.parse_args()

    return args

if __name__ == '__main__':

    args = parse_arguments()
    extract_features(args.input, args.output, args.hadoop_output, args.max_words_summary, args.max_words_review, env='hadoop')
    print('Done!')