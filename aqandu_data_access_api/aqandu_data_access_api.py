from flask import Flask
from flask_compress import Compress
from flask_cors import CORS

from .influx.influx import influx
from .mongo.mongo import mongo

app = Flask(__name__, instance_relative_config=True)   # create the application instance
app.config.from_object('config')
app.config.from_pyfile('config.py')

# register the blueprints
app.register_blueprint(influx)
app.register_blueprint(mongo)


Compress(app)
CORS(app)
# sentry = Sentry(app, dsn=config['sentry'])


# # if __name__ == "__main__":
#     app.run(debug=True)
