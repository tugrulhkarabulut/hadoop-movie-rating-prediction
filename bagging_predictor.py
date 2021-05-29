from mrjob.job import MRJob
from time import time
import sys



class BaggingPredictor(MRJob):
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
        import scipy.stats as st

        yield key, list(st.mode(values, axis=0)[0][0])[0]


if __name__ == '__main__':
    BaggingPredictor.run()