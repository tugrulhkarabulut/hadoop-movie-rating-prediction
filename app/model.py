from flask import Blueprint
from flask import request
from ..feature_extractor import *

bp = Blueprint('model', __name__)


@bp.route('/extract', methods=['POST'])
def extract():
    req_data = request.get_json()
    a = {'id': 123}
    return a

@bp.route('/build', methods=['POST'])
def build():
    req_data = request.get_json()
    a = {'id': 123}
    return a