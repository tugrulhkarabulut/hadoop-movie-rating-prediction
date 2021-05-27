from mrjob.job import MRJob
import sys

class BaseJob(MRJob):
    OUTPUT_PATH = None
    INPUT_PATH = None
    COLUMN_INDEX = 3
    
    @classmethod
    def set_column_index(cls, column_index):
        cls.COLUMN_INDEX = column_index

    @classmethod
    def set_output_path(cls, output_path):
        cls.OUTPUT_PATH = output_path

    @classmethod
    def set_input_path(cls, input_path):
        cls.INPUT_PATH = input_path

    @classmethod
    def set_column_index(cls, column_index):
        cls.COLUMN_INDEX = column_index

    @classmethod
    def run(cls):
        sys.stdin = open(cls.INPUT_PATH, 'r')
        sys.stdout = open(cls.OUTPUT_PATH, 'w')
        super(BaseJob, cls).run()
        sys.stdout.close()
        sys.stdin.close()

        sys.stdin = sys.__stdin__
        sys.stdout = sys.__stdout__