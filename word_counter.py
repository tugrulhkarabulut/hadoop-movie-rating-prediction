from mrjob.job import MRJob
from time import time
import sys

class WordCounter(MRJob):
    COLUMN_INDEX = 3

    def mapper(self, _, line):
        try:
            splitted = line.split(',')
            review_summary = splitted[WordCounter.COLUMN_INDEX]
            for word in review_summary.strip().split():
                yield word, 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    WordCounter.run()
