from mrjob.job import MRJob


class TermFrequencyCalculator(MRJob):
    def configure_args(self):
        super(TermFrequencyCalculator, self).configure_args()
        self.add_passthru_arg('--column_index', type=int, default=3)
        self.add_passthru_arg('--words', type=str)

    def load_args(self, args):
        super(TermFrequencyCalculator, self).load_args(args)

    def set_max_words(self, max_words):
        self.MAX_WORDS = max_words

    def set_word_counts(self, counts):
        filtered = counts.sort_values(by='count', ascending=False).iloc[:self.MAX_WORDS].reset_index(drop=True)
        filtered.index = filtered['word']
        del filtered['word']
        self.counts = filtered

    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[self.options.column_index]
        review_id = splitted[0]
        words = review_summary.strip().split()
        for word in words:
            if word in self.options.words.split(','):
                yield (review_id, word), 1 / len(words)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TermFrequencyCalculator.run()
