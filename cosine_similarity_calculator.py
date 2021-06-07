from mrjob.job import MRJob
import math

class CosineSimilarityCalculator(MRJob):

    def configure_args(self):
        super(CosineSimilarityCalculator, self).configure_args()
        self.add_passthru_arg('--doc_features', type=str)

    def load_args(self, args):
        super(CosineSimilarityCalculator, self).load_args(args)


    def mapper(self, _, line):
        orig_doc_feats = [float(feat) for feat in self.options.split(',')]
        orig_doc_norm = math.sqrt([f ** 2 for f in orig_doc_feats])

        line_values = line.split(',')
        id = line_values[0]
        doc_feats = [float(feat) for feat in line_values[1:]]
        doc_norm = math.sqrt([f ** 2 for f in doc_feats])

        for i in range(len(doc_feats)):
            norm_multiple = orig_doc_norm * doc_norm

            if norm_multiple == 0:
                norm_multiple = 1

            yield id, orig_doc_feats[i] * doc_feats[i] / (orig_doc_norm * doc_norm)

        
    def reducer(self, key, values):
        yield key, math.cos(sum(values))



if __name__ == '__main__':
    CosineSimilarityCalculator.run()