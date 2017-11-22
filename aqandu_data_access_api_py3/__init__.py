from flask import Flask
from flask_compress import Compress
from flask_cors import CORS
# from raven.contrib.flask import Sentry

from aqandu_data_access_api_py3.influx.influx import influx
from aqandu_data_access_api_py3.mongo.mongo import mongo

app = Flask(__name__, instance_relative_config=True)   # create the application instance
app.config.from_object('config')
app.config.from_pyfile('config.py')

# register the blueprints
app.register_blueprint(influx)
app.register_blueprint(mongo)


Compress(app)
# CORS(app)
# sentry = Sentry(app, dsn=app.config['SENTRY'])


@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There! Just checking that it works.</h1>"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
