from flask import Blueprint
from flask import request
from ..feature_extractor import *
from ..train_model import *
from pydoop import hdfs
import shutil
import os
import json

bp = Blueprint('model', __name__)


@bp.route('/extract', methods=['POST'])
def extract():
    req_data = request.get_json()
    process_name = req_data['name']
    input_path = ''
    process_path = './app_data/' + process_name

    shutil.rmtree(process_path, ignore_errors=True)
    os.mkdir(process_path)

    hadoop_output = '/input/' + process_name + '_output.csv'
    env = req_data['env']
    dataset_input = req_data['dataset_input']
    feature_types = req_data['feature_types'] 

    if dataset_input == 'small':
        if env == 'multi':
            input_path = 'hdfs:///input/preprocessed_sample.csv'
        else:
            input_path = 'hdfs:///input/preprocessed_sample_single.csv'
    else:
        if env == 'multi':
            input_path = 'hdfs:///input/preprocessed_part1.csv'
        else:
            input_path = 'hdfs:///input/preprocessed_part1_single.csv'


    print(input_path)

    model_config = {
        "input_path": input_path,
        "env": env,
        "dataset": dataset_input,
        "hadoop_output": "hdfs://" + hadoop_output,
        "feature_types": feature_types
    }
    with open(process_path + '/model.json', 'w') as f:
        json.dump(model_config, f)

    extract_features(process_path, input_path, hadoop_output, feature_types)

    response = {'process_name': process_name}

    return response

@bp.route('/build', methods=['POST'])
def build():

    req_data = request.get_json()
    process_name = req_data['process_name']
    process_path = './app_data/' + process_name
    model_output = process_path + '/model.pickle'

    train_input = '/input/' + process_name + '_output.csv'

    train_acc, test_acc = train_model(train_input, model_output)

    response = {'train_acc': train_acc, 'test_acc': test_acc}
    return response