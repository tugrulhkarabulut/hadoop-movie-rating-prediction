from flask import Blueprint
from flask import request
from ..feature_extractor import *
from ..train_model import *
from pydoop import hdfs
import shutil

bp = Blueprint('model', __name__)


@bp.route('/extract', methods=['POST'])
def extract():
    req_data = request.get_json
    process_name = req_data['name']
    input_path = ''
    process_path = '../app_data/' + process_name

    shutil.rmtree(process_path, ignore_errors=True)

    output_path = process_path + '/output.csv'
    hadoop_output = '/input/' + process_name + '_output.csv'
    env = req_data['env']
    dataset_input = req_data['dataset_input']
    feature_types = req_data['feature_types']

    if dataset_input == 'small':
        input_path = 'hdfs:///input/preprocessed_sample.csv'
    else:
        input_path = 'hdfs:///input/preprocessed_part1.csv'


    if env == 'local':
        with hdfs.open(input_path) as f:
            if dataset_input == 'small':
                input_path = process_path + '/preprocessed_sample.csv'
                pd.read_csv(f).to_csv(input_path)
            else:
                input_path = process_path + '/preprocessed_part1.csv'
                pd.read_csv(f).to_csv(input_path)



    extract_features(input_path, output_path, hadoop_output, feature_types)

    response = {'process_name': process_name}

    return response

@bp.route('/build', methods=['POST'])
def build():

    req_data = request.get_json()
    process_name = req_data['process_name']
    process_path = '../app_data/' + process_name
    env = req_data['env']
    model_output = process_path + '/model.pickle'

    if env == 'hadoop':
        train_input = process_path + '/output.csv'
    else:
        train_input = '/input/' + process_name + '_output.csv'

    train_acc, test_acc = train_model(train_input, model_output, env)

    response = {'train_acc': train_acc, 'test_acc': test_acc}
    return response