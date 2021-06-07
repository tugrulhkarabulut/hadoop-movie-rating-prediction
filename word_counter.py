from mrjob.job import MRJob
import csv
from io import StringIO

class WordCounter(MRJob):    
    def configure_args(self):
        super(WordCounter, self).configure_args()
        self.add_passthru_arg('--column_index', type=int, default=3)

    def load_args(self, args):
        super(WordCounter, self).load_args(args)

    def mapper(self, _, line):
        splitted = list(csv.reader(StringIO(line), skipinitialspace=True))[0]
        review_summary = splitted[int(self.options.column_index)]
        for word in review_summary.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    WordCounter.run()
