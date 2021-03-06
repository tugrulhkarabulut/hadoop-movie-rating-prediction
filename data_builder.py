import pandas as pd


class DataBuilder:
    def __init__(self, orig_data_input=None, tf_idf_inputs=None):
        self.orig_data_input = orig_data_input
        self.tf_idf_inputs = tf_idf_inputs
    
    def load_data(self):
        self.data = pd.read_csv(self.orig_data_input)
        self.tf_idf_data = []
        for inp in self.tf_idf_inputs:
            self.tf_idf_data.append(pd.read_csv(inp))
    
    def build(self):
        if not hasattr(self, 'data'):
            self.load_data()

        for i, inp in enumerate(self.tf_idf_data):
            columns = inp.columns
            pivot = inp.groupby(['review_id', columns[1]])[columns[2]].first().unstack()
            print('pivot done')
            print(pivot.shape)
            pivot = pivot.add_suffix('_{}'.format(i))
            self.data = self.data.join(pivot, on='review_id', how='left')
            print('join done')
            self.data = self.data.fillna(0)

    def save(self, output_path):
        self.data.to_csv(output_path, index=False)