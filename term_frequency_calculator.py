from base_job import BaseJob
from time import time
import sys
import pandas as pd



class TermFrequencyCalculator(BaseJob):
    COUNTS = None
    MAX_WORDS = 50

    @classmethod
    def set_max_words(cls, max_words):
        cls.MAX_WORDS = max_words

    @classmethod
    def read_word_counts(cls, input_path):
        counts = pd.read_csv(input_path, sep='\t', header=None)
        filtered = counts.sort_values(by=1, ascending=False).iloc[:TermFrequencyCalculator.MAX_WORDS].reset_index(drop=True)
        filtered.index = filtered[0]
        del filtered[0]
        cls.COUNTS = filtered

    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[TermFrequencyCalculator.COLUMN_INDEX]
        review_id = splitted[0]
        words = review_summary.strip().split()
        for word in words:
            if word in TermFrequencyCalculator.COUNTS.index:
                yield (review_id, word), 1 / len(words)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TermFrequencyCalculator.set_input_path('preprocessed_sample.csv')
    TermFrequencyCalculator.read_word_counts('review_summary_counts.txt')
    TermFrequencyCalculator.set_output_path('review_summary_term_frequencies.txt')
    TermFrequencyCalculator.run()
    
    TermFrequencyCalculator.set_column_index(5)
    TermFrequencyCalculator.set_max_words(100)
    TermFrequencyCalculator.read_word_counts('review_detail_counts.txt')
    TermFrequencyCalculator.set_output_path('review_detail_term_frequencies.txt')
    TermFrequencyCalculator.set_input_path('preprocessed_sample.csv')
    TermFrequencyCalculator.run()
