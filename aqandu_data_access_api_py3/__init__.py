from flask import Flask
from flask_compress import Compress
from flask_mail import Mail
# from flask_cors import CORS
import logging
import logging.handlers as handlers
# from raven.contrib.flask import Sentry

app = Flask(__name__, instance_relative_config=True)   # create the application instance
app.config.from_object('config')
app.config.from_pyfile('config.py')

mail = Mail(app)

# register the blueprints
from aqandu_data_access_api_py3.influx.influx import influx
from aqandu_data_access_api_py3.mongo.mongo import mongo

app.register_blueprint(influx)
app.register_blueprint(mongo)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - [%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s')

# theFile = logging.FileHandler('aqanduAPI.log')
# theFile.setLevel(logging.INFO)
# logger.addHandler(theFile)

logHandler = handlers.TimedRotatingFileHandler('aqanduAPI.log', when='h', interval=6, backupCount=5)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
app.logger.addHandler(logHandler)


Compress(app)
# CORS(app)
# sentry = Sentry(app, dsn=app.config['SENTRY'])


# getting to this page with url: http://air.eng.utah.edu/dbapi/
@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There! Just checking that it works.</h1>"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
