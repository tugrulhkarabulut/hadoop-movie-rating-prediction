from base_job import BaseJob
from time import time
import sys
import pandas as pd
import scipy.stats as st


class BaggingPredictor(BaseJob):
    CALC_FUNCTION = None
    CLASSIFIERS = None

    @classmethod
    def set_calc_function(cls, func):
        cls.CALC_FUNCTION = func

    @classmethod
    def set_classifiers(cls, classifiers):
        cls.CLASSIFIERS = classifiers

    def mapper(self, _, line):
        preds = BaggingPredictor.CALC_FUNCTION(BaggingPredictor.CLASSIFIERS[int(line)])
        for i, el in enumerate(preds):
            yield str(i), int(el)

    def reducer(self, key, values):
        yield key, list(st.mode(values, axis=0)[0][0])[0]
