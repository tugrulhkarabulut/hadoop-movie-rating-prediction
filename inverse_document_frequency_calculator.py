from mrjob.job import MRJob

class InverseDocumentFrequencyCalculator(MRJob):
    @classmethod
    def calc_n_rows(cls, f):
        return sum(1 for _ in f)

    def configure_args(self):
        super(InverseDocumentFrequencyCalculator, self).configure_args()
        self.add_passthru_arg('--column_index', type=int, default=3)
        self.add_passthru_arg('--n_rows', type=int)

    def load_args(self, args):
        super(InverseDocumentFrequencyCalculator, self).load_args(args)


    def mapper(self, _, line):
        import numpy as np

        splitted = line.split(',')
        review_summary = splitted[self.options.column_index]
        words = review_summary.strip().split()
        unique_words = np.unique(words)
        for word in unique_words:
            yield word, 1

    def reducer(self, key, values):
        import numpy as np

        yield key, np.log(int(self.options.n_rows) / (sum(values) + 10e-8))



if __name__ == '__main__':
    InverseDocumentFrequencyCalculator.run()