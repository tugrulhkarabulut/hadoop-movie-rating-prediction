from mrjob.job import MRJob
import re
import csv
from io import StringIO

class NGramLocalCounter(MRJob):    
    def configure_args(self):
        super(NGramLocalCounter, self).configure_args()
        self.add_passthru_arg('--n_grams', type=str)
        self.add_passthru_arg('--column_index', type=int, default=3)

    def load_args(self, args):
        super(NGramLocalCounter, self).load_args(args)

    def mapper(self, _, line):
        splitted = list(csv.reader(StringIO(line), skipinitialspace=True))[0]
        review_summary = splitted[int(self.options.column_index)]
        review_id = splitted[0]
        n_grams = self.options.n_grams.split(',')
        try:
            for n_gram in n_grams:
                n_gram_spaced = ' '.join(n_gram.split('_'))
                res = re.search(n_gram_spaced, review_summary)
                exists = 1 if res else 0
                yield (review_id, n_gram), exists
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NGramLocalCounter.run()
