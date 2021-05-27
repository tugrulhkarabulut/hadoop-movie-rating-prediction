from base_job import BaseJob
from mrjob.job import MRJob
from time import time
import numpy as np

class InverseDocumentFrequencyCalculator(BaseJob):
    N_ROWS = None

    def calc_n_rows():
        with open(InverseDocumentFrequencyCalculator.INPUT_PATH) as f:
            InverseDocumentFrequencyCalculator.N_ROWS = sum(1 for _ in f)


    def mapper(self, _, line):
        splitted = line.split(',')
        review_summary = splitted[InverseDocumentFrequencyCalculator.COLUMN_INDEX]
        words = review_summary.strip().split()
        unique_words = np.unique(words)
        for word in unique_words:
            yield word, 1

    def reducer(self, key, values):
        yield key, np.log(InverseDocumentFrequencyCalculator.N_ROWS / (sum(values) + 10e-8))



if __name__ == '__main__':
    InverseDocumentFrequencyCalculator.set_input_path('preprocessed_sample.csv')
    InverseDocumentFrequencyCalculator.calc_n_rows()
    InverseDocumentFrequencyCalculator.set_output_path('review_summary_document_counts.txt')
    InverseDocumentFrequencyCalculator.run()
    InverseDocumentFrequencyCalculator.set_column_index(5)
    InverseDocumentFrequencyCalculator.set_output_path('review_detail_document_counts.txt')
    InverseDocumentFrequencyCalculator.run()