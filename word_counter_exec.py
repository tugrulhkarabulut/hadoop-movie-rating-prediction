#!/usr/bin/python3

from word_counter import WordCounter

if __name__ == '__main__':
    w = WordCounter(args=['hdfs:///input/preprocessed_sample.csv', '-r', 'hadoop'])
    with w.make_runner() as runner:
        runner.run()
        word_count_dict = {}
        for key, value in w.parse_output(runner.cat_output()):
            word_count_dict[key] = value


