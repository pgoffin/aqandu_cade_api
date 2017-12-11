import time
import logging

from datetime import datetime
from flask import jsonify, request, Blueprint
from flask import current_app
from pymongo import MongoClient
from twilio.rest import Client

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


# http://air.eng.utah.edu/dbapi/api/sensorIsConnected?sensor_mac=F4:5E:AB:9C:02:DF&email=pink@sci.com&phone=+8015583223&mapVisibility=true
@mongo.route('/api/sensorIsConnected', methods=['POST'])
def sensorIsConnected():

    LOGGER.info('sensorIsConnected POST request started')

    print('testing6')
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    # TWILIO client
    client = Client(current_app.config['TWILIO_ACCOUNT_SID'], current_app.config['TWILIO_AUTH_TOKEN'])

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
        print('testing4')
        aSensor = {"sensor_mac": queryParameters['mac'],
                   "email": queryParameters['email'],
                   "phone": queryParameters['phone'],
                   "mapVisibility": queryParameters['mapVisibility'],
                   "created_at": now}

        print('testing1')
        sendMessage(client, current_app.config['PHONE_NUMBER_TO_SEND_MESSAGE'])
        print('testing2')
        db.sensors.insert_one(aSensor)
        print('testing3')



        end = time.time()

        print("*********** Time to insert:", end - start)

        return jsonify(message='The sensor was registered.')
    except Exception:
        return jsonify(message='An error occurred.')


def sendMessage(twilioClient, messageFrom):
    print('message being sent')

    message = twilioClient.messages.create(
        to = "+18015583223",
        from_ = messageFrom,
        body = "Hello from AQandU!")

    print(message.sid)
