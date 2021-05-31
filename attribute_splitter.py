from mrjob.job import MRJob
import pickle
import os
import tarfile

class AttributeSplitter(MRJob):

    FILES = ['test.py']
    DIRS = ['~/.local/lib/python3.6/site-packages/numpy#my_numpy']

    def configure_args(self):
        super(AttributeSplitter, self).configure_args()
        self.add_passthru_arg('--criterion', type=str)
        self.add_file_arg('--split_data')
        self.add_file_arg('--tree')

    def load_args(self, args):
        super(AttributeSplitter, self).load_args(args)

    def mapper(self, _, line):
        nump_f = tarfile.open('numpy.tar.gz', "r:gz")
        nump_f.extractall()
        nump_f.close()

        from test import test_func
        test_func()
        with open(self.options.split_data) as f:
            pass
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