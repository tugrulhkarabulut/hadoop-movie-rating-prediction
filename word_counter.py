from mrjob.job import MRJob
from time import time
import sys

class WordCounter(MRJob):
    COLUMN_INDEX = 3

    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[WordCounter.COLUMN_INDEX]
        for word in review_summary.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)
