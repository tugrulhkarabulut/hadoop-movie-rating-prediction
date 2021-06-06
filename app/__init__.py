import os

from flask import Flask, render_template, request

def create_app():   
    # create and configure the app
    app = Flask(__name__)

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