import time
import logging

from datetime import datetime
from flask import jsonify, request, Blueprint
# from influxdb import InfluxDBClient
from pymongo import MongoClient

# from .. import app
from flask import current_app

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

mongo = Blueprint('mongo', __name__)


# adds sensor data to the db
# def add_sensor(mac_address, email):
#     now = datetime.utcnow()
#     print(now)
#
#     theCollection.insert_one({"sensor_mac": mac_address,
#                         "sensor_holder": email,
#                         "created_at": now})


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

    # Do parameter checking

    try:
        start = time.time()
        print(queryParameters['sensor_mac'], queryParameters['sensor_holder'])

        now = datetime.utcnow()
        print(now)

        aResult = db.sensors.find_one()
        print(aResult)
        LOGGER.info("Adding event: %s", aResult)

        aSensor = {"sensor_mac": queryParameters['sensor_mac'],
                   "sensor_holder": queryParameters['sensor_holder'],
                   "created_at": now}

        db.sensors.insert_one(aSensor)

        # add_sensor(queryParameters['sensor_mac'], queryParameters['sensor_holder'])

        end = time.time()

        print("*********** Time to insert:", end - start)

        return jsonify(message='The event was added.')
    except Exception:
        return jsonify(message='An error occurred.')


# @app.route('/event', methods=['POST'])
# def set_event():
#     url_data = request.args
#     data = request.json
#
#     if data is None:
#         return jsonify(error="must provide data"), 400
#
#     if 'text' not in data:
#         return jsonify(error="'text' must be provided"), 400
#
#     if 'time' not in data:
#         return jsonify(error="'time' must be provided"), 400
#
#     if 'source' not in data:
#         return jsonify(error="'source' must be provided"), 400
#
#     try:
#         time = arrow.get(data['time'])
#     except arrow.parser.ParserError:
#         return jsonify(error="Unable to parse time: {}".format(data['time'])), 400
#
#     try:
#         deployment_info = get_deployment(url_data)
#     except ApiException as e:
#         return jsonify(error=str(e)), 400
#
#     try:
#         add_event(data['text'], data['source'], time, deployment_info)
#         return jsonify(message='The event was added.')
#     except Exception:
#         return jsonify(message='An error occurred.')
