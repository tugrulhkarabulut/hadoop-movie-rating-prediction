from base_job import BaseJob
from time import time
import sys
import pandas as pd

class AttributeSplitter(BaseJob):
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
