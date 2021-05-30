from mrjob.job import MRJob
import pickle

class AttributeSplitter(MRJob):

    def configure_args(self):
        super(AttributeSplitter, self).configure_args()
        self.add_passthru_arg('--criterion', type=str)
        #self.add_file_arg('--split_data')
        #self.add_file_arg('--tree')

    def load_args(self, args):
        super(AttributeSplitter, self).load_args(args)

    def mapper(self, _, line):
        from decision_tree import calculate_split_result
        from random_forest_classifier import RandomForestClassifier
        try:
            yield int(line), (0, 0.5)
        except:
            pass

    def reducer(self, key, values):
        values = list(values)
        values.sort(key=lambda x: x[1])
        yield key, (values[0][0], values[0][1])


if __name__ == '__main__':
    AttributeSplitter.run()