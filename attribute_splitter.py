from mrjob.job import MRJob
from time import time
import sys

class AttributeSplitter(MRJob):
    CALC_FUNCTION = None

    @classmethod
    def set_calc_function(cls, func):
        cls.CALC_FUNCTION = func

    def mapper(self, _, line):
        yield AttributeSplitter.CALC_FUNCTION(int(line))

    def reducer(self, key, values):
        values = list(values)
        values.sort(key=lambda x: x[1])
        yield key, (values[0][0], values[0][1])


if __name__ == '__main__':
    AttributeSplitter.run()