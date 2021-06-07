from mrjob.job import MRJob
import re

class ExclamationMarkCounter(MRJob):    
    def configure_args(self):
        super(ExclamationMarkCounter, self).configure_args()
        self.add_passthru_arg('--column_index', type=int, default=3)

    def load_args(self, args):
        super(ExclamationMarkCounter, self).load_args(args)

    def mapper(self, _, line):
        regex = r'[\?\!]'
        splitted = line.split(',')
        review_summary = splitted[int(self.options.column_index)]
        review_id = splitted[0]

        for mark in re.findall(regex, review_summary.strip()):
            yield (review_id, mark), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    ExclamationMarkCounter.run()
