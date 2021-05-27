from word_counter import WordCounter

if __name__ == '__main__':
    w = WordCounter(args=['-r', 'hadoop', '--files', 'hdfs:///input/preprocessed_sample.csv'])
    with w.make_runner() as runner:
        runner.run()
        word_count_dict = {}
        for key, value in w.parse_output(runner.cat_output()):
            word_count_dict[key] = value

        print(len(word_count_dict))

        WordCounter.COLUMN_INDEX = 5

        runner.run()
        word_count_dict = {}
        for key, value in w.parse_output(runner.cat_output()):
            word_count_dict[key] = value

        print(len(word_count_dict))


