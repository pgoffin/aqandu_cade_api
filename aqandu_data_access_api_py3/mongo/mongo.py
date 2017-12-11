import time
# import logging
import distutils

from datetime import datetime
from flask import jsonify, request, Blueprint
from flask import current_app
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from pymongo import MongoClient
from twilio.rest import Client
from werkzeug.local import LocalProxy

# logging.basicConfig(level=logging.DEBUG)
# LOGGER = logging.getLogger(__name__)
LOGGER = LocalProxy(lambda: current_app.logger)

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

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    influxClientLoggingSensorConnections = InfluxDBClient(
                host=current_app.config['INFLUX_HOST'],
                port=current_app.config['INFLUX_PORT'],
                username=current_app.config['INFLUX_USERNAME'],
                password=current_app.config['INFLUX_PASSWORD'],
                database=current_app.config['INFLUX_AIRU_LOGGING_SENSOR_CONNECTION_DATABASE'],
                ssl=current_app.config['SSL'],
                verify_ssl=current_app.config['SSL'])

    # TWILIO client
    client = Client(current_app.config['TWILIO_ACCOUNT_SID'], current_app.config['TWILIO_AUTH_TOKEN'])

    queryParameters = request.get_json()
    LOGGER.info(queryParameters)

    # TODO Do parameter checking
    # TODO Add the request to an influxdb (logging)
    # TODO https://stackoverflow.com/questions/3759981/get-ip-address-of-visitors
    # TODO https://stackoverflow.com/questions/33818540/how-to-get-the-first-client-ip-from-x-forwarded-for-behind-nginx-gunicorn?noredirect=1&lq=1

    try:
        start = time.time()
        now = datetime.utcnow()
        LOGGER.info('testing4')

        # TODO check if mac,email or mac,phone exists already
        # if it already exists do nothin
        # if mac exists, but phone or email is new update
        # if mac does not yet exist insert

        aSensor = {"sensor_mac": queryParameters['mac'],
                   "email": queryParameters['email'],
                   "phone": queryParameters['phone'],
                   "mapVisibility": queryParameters['mapVisibility'],
                   "created_at": now}

        aMeasurement = {
            'measurement': current_app.config['INFLUX_AIRU_LOGGING_SENSOR_MEASUREMENT'],
            'fields': {
                'email': queryParameters['email'],
                'phone': queryParameters['phone'],
                'mapVisibility': bool(distutils.util.strtobool(queryParameters['mapVisibility']))
            },
            'tags': {
                'MACaddress': queryParameters['mac']
            }
        }

        LOGGER.info(aMeasurement)
        influxClientLoggingSensorConnections.write_points([aMeasurement])

        LOGGER.info('testing1')
        sendMessage(client, current_app.config['PHONE_NUMBER_TO_SEND_MESSAGE'], queryParameters['mac'])
        LOGGER.info('testing2')
        db.sensors.insert_one(aSensor)
        LOGGER.info('testing3')

        end = time.time()



        LOGGER.info("*********** Time to insert:", end - start)

        return jsonify(message='The sensor was registered.')
    except InfluxDBClientError as e:
        LOGGER.error('InfluxDBClientError:\tWriting to influxdb lead to a write error.')
        LOGGER.error(aSensor)
        LOGGER.error(e)
    except Exception:
        return jsonify(message='An error occurred.')


def sendMessage(twilioClient, messageFrom, mac):
    LOGGER.info('message being sent')

    message = twilioClient.messages.create(
        to="+18015583223",
        from_=messageFrom,
        body="Hello from AQandU! Your sensor " + mac + " is now connected!!")

    LOGGER.info(message.sid)
