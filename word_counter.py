from mrjob.job import MRJob
from base_job import BaseJob
from time import time
import sys

class WordCounter(BaseJob):
    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[WordCounter.COLUMN_INDEX]
        for word in review_summary.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    WordCounter.set_input_path('preprocessed_sample.csv')
    WordCounter.set_output_path('review_summary_counts.txt')
    WordCounter.run()
    WordCounter.set_output_path('review_detail_counts.txt')
    WordCounter.set_column_index(5)
    WordCounter.run()
