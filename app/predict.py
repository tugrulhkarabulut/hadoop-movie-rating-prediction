from flask import Blueprint
from flask import request
import os

bp = Blueprint('predict', __name__)

@bp.route('/predict', methods=['POST'])
def build():
    req_data = request.get_json()
    a = {'id': 123}
    return a


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