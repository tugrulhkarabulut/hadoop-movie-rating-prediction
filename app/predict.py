from flask import Blueprint
from flask import request

bp = Blueprint('predict', __name__)


@bp.route('/predict', methods=['POST'])
def build():
    req_data = request.get_json()
    a = {'id': 123}
    return a