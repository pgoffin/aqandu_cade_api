from flask import Flask, jsonify
from flask_compress import Compress
from flask_mail import Mail
# from flask_cors import CORS
import logging
import logging.handlers as handlers
# from raven.contrib.flask import Sentry
from werkzeug.exceptions import HTTPException, default_exceptions

app = Flask(__name__, instance_relative_config=True)   # create the application instance
app.config.from_object('config')
app.config.from_pyfile('config.py')

mail = Mail(app)

# register the blueprints
from aqandu_data_access_api_py3.influx.influx import influx
from aqandu_data_access_api_py3.mongo.mongo import mongo

app.register_blueprint(influx)
app.register_blueprint(mongo)


# to remove debug logs from flask to be logged in the gunicorn logs
del app.logger.handlers[:]

# logger = logging.getLogger(__name__)
logger = logging.getLogger('aqandu')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - [%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s')

# to remove debug logs from flask to be logged in the gunicorn logs
# del app.logger.handlers[:]

logHandler = handlers.TimedRotatingFileHandler('aqanduAPI.log', when='h', interval=6, backupCount=5)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# adding a logger for uncaught exceptions
# uncaughtExcpt_logger = logging.getLogger('uncaughtExcpt')
# uncaughtExcpt_logger.setLevel(logging.INFO)

# uncaughtExcpt_logHandler = logging.Formatter('%(asctime)s - %(name)s - [%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s')
# uncaughtExcpt_logHandler = handlers.TimedRotatingFileHandler('uncaughtErrors.log', when='h', interval=6, backupCount=5)
# uncaughtExcpt_logHandler.setLevel(logging.INFO)
# uncaughtExcpt_logHandler.setFormatter(uncaughtExcpt_logHandler)
# app.logger.addHandler(uncaughtExcpt_logHandler)


def handle_error(error):
    code = 500
    if isinstance(error, HTTPException):
        code = error.code
    return jsonify(error='error', code=code)


for exc in default_exceptions:
    app.register_error_handler(exc, handle_error)

Compress(app)
# CORS(app)
# sentry = Sentry(app, dsn=app.config['SENTRY'])


# getting to this page with url: http://air.eng.utah.edu/dbapi/
@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There! Just checking that it works.</h1>"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
