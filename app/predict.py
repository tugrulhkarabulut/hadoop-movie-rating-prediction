from flask import Blueprint
from flask import request
import os

bp = Blueprint('predict', __name__)

@bp.route('/predict', methods=['POST'])
def build():
    req_data = request.get_json()
    process_name = req_data['process_name']
    process_path = './app_data/' + process_name

    feature_types = []
    with open(process_path + '/feature_types.txt') as f:
        for line in f:
            feature_types.append(line)

    print(feature_types)
    return {}


@bp.route('/get-models', methods=['GET'])
def get_models():
    models = []
    model_names = os.listdir('./app_data')

    for m in model_names:
        if os.listdir('./app_data/' + m):
            models.append(m)
    

    res = {}
    res['models'] = models

    return res