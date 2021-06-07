import os

from flask import Flask, render_template, request


class VueFlask(Flask):
    jinja_options = Flask.jinja_options.copy()
    jinja_options.update(dict(
        variable_start_string='%%',
        variable_end_string='%%',
    ))

def create_app():   
    # create and configure the app
    app = VueFlask(__name__)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def landing():
        return render_template('index.html')

    from . import model, predict
    app.register_blueprint(model.bp)
    app.register_blueprint(predict.bp)

    return app