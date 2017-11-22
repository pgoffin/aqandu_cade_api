import time
import logging

from datetime import datetime
from flask import jsonify, request, Blueprint
from pymongo import MongoClient

from flask import current_app

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

mongo = Blueprint('mongo', __name__)


# http://air.eng.utah.edu/dbapi/api/registerSensor
@mongo.route('/api/registerSensor', methods=['POST'])
def registerSensor():

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    print(db)

    # queryParameters = request.args
    # print(queryParameters)
    # test1 = request.get_json(force=True)
    # print(test1)
    queryParameters = request.get_json()
    print(queryParameters)

    # TODO  Do parameter checking

    try:
        start = time.time()
        now = datetime.utcnow()

        aSensor = {"sensor_mac": queryParameters['sensor_mac'],
                   "sensor_holder": queryParameters['sensor_holder'],
                   "created_at": now}

        db.sensors.insert_one(aSensor)

        end = time.time()

        print("*********** Time to insert:", end - start)

        return jsonify(message='The sensor was registered.')
    except Exception:
        return jsonify(message='An error occurred.')
