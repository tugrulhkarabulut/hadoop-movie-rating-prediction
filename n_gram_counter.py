from mrjob.job import MRJob

class NGramCounter(MRJob):    
    def configure_args(self):
        super(NGramCounter, self).configure_args()
        self.add_passthru_arg('--n', type=int, default=2)
        self.add_passthru_arg('--column_index', type=int, default=3)

    def load_args(self, args):
        super(NGramCounter, self).load_args(args)

    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[int(self.options.column_index)]
        words = review_summary.strip().split()
        n = self.options.n
        try:
            for i in range(len(words) - n + 1):
                yield '_'.join(words[i : i + n]), 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NGramCounter.run()
