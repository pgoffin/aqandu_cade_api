import time
# import logging
import distutils
from threading import Thread

from datetime import datetime
from flask import jsonify, request, Blueprint
from flask import current_app
from flask_mail import Message
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from pymongo import MongoClient
from twilio.rest import Client
from werkzeug.local import LocalProxy

from aqandu_data_access_api_py3 import mail, app

# logging.basicConfig(level=logging.DEBUG)
# LOGGER = logging.getLogger(__name__)
LOGGER = LocalProxy(lambda: current_app.logger)
# theMail = LocalProxy(lambda: current_app.mail)

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
    # TODO add the tweak to use threads: http://www.patricksoftwareblog.com/sending-email/
    # TODO check if the MAC address is in our list of MAC addresses

    try:
        now = datetime.utcnow()

        aSensor = {"macAddress": queryParameters['mac'],
                   "email": queryParameters['email'],
                   "phone": queryParameters['phone'],
                   "mapVisibility": queryParameters['mapVisibility'],
                   "createdAt": now}

        sensorConnectionMeasurement = {
            'measurement': current_app.config['INFLUX_AIRU_LOGGING_SENSOR_MEASUREMENT'],
            'fields': {
                'email': queryParameters['email'],
                'mapVisibility': bool(distutils.util.strtobool(queryParameters['mapVisibility'])),
                'phone': queryParameters['phone']
            },
            'tags': {
                'macAddress': queryParameters['mac']
            }
        }

        LOGGER.info(sensorConnectionMeasurement)
        startInfluxWrite = time.time()

        # logging every connection attempt
        influxClientLoggingSensorConnections.write_points([sensorConnectionMeasurement])
        LOGGER.info('testing1')

        endInfluxWrite = time.time()
        LOGGER.info('testing2')
        timeToWriteInflux = endInfluxWrite - startInfluxWrite
        LOGGER.info('testing3')
        LOGGER.info('*********** Time to write to influx: %s', timeToWriteInflux)
        LOGGER.info('testing4')

        # check if already entry with given MAC address if no insert, if yes more checks
        startMongoWrite = time.time()
        entryWithGivenMAC = db.sensors.find_one({'sensorMac': queryParameters['mac']})
        LOGGER.info('testing5')
        LOGGER.info(entryWithGivenMAC)
        if entryWithGivenMAC is None:
            db.sensors.insert_one(aSensor)
            LOGGER.info(queryParameters['mac'] + ' inserted into Mongo db')
        else:
            # a mac address will always have only one entry, if there is already an entry replace it with the new entry
            db.sensors.replace_one({'_id': entryWithGivenMAC['_id']}, aSensor)
            LOGGER.info(queryParameters['mac'] + ' was already present. Replaced with new information.')

        endMongoWrite = time.time()
        timeToWriteMongo = endMongoWrite - startMongoWrite
        LOGGER.info("*********** Time to write to Mongo:" + timeToWriteMongo)

        #  if there is a phone number prefer phone
        if queryParameters['phone'] != '':
            LOGGER.info('sending a text')
            startSendText = time.time()

            sender = current_app.config['PHONE_NUMBER_TO_SEND_MESSAGE']
            recipient = queryParameters['email']
            theMessage = 'Hello from AQandU! Your sensor with MAC address ' + queryParameters['mac'] + ' is now connected to the internet and is gathering data. Thank you for participating!'
            sendText(client, sender, recipient, theMessage)

            endSendText = time.time()
            timeToSendText = endSendText - startSendText
            LOGGER.info("*********** Time to send text:" + timeToSendText)

        elif queryParameters['email'] != '':

            LOGGER.info('sending an email')
            startSendEmail = time.time()

            aSubject = 'AQandU sensor is connected'
            recipients = [queryParameters['email']]
            # textBody = 'Hello from AQandU! Your sensor with MAC address ' + queryParameters['mac'] + ' is now connected to the internet and is gathering data. Thank you for participating!'

            sendEmail(aSubject, recipients, theMessage)
            # msg = Message(subject='AQandU sensor is connected',
            #               body='Hello from AQandU!! Your sensor is now connected to the internet and is gathering data. Thank you for participating!',
            #               recipients=[queryParameters['email']])

            LOGGER.info('testing3')
            # mail.send(msg)
            endSendEmail = time.time()
            timeToSendEmail = endSendEmail - startSendEmail
            LOGGER.info("*********** Time to send Email:" + timeToSendEmail)

        else:
            LOGGER.info('no phone number and no email address')

        return jsonify(message='The sensor was registered.')

    except InfluxDBClientError as e:
        LOGGER.error('InfluxDBClientError:\tWriting to influxdb lead to a write error.')
        LOGGER.error(aSensor)
        LOGGER.error(e)
    except Exception:
        return jsonify(message='An error occurred.')


def sendText(twilioClient, sender, recipient, message):
    """Sends a text"""
    LOGGER.info('Text is being sent.')

    message = twilioClient.messages.create(
        to=recipient,
        from_=sender,
        body=message)

    LOGGER.info(message.sid)


def sendAsyncEmail(msg):
    with app.app_context():
        mail.send(msg)


def sendEmail(subject, recipients, text_body):
    """Sends an email"""
    msg = Message(subject, recipients=recipients)
    msg.body = text_body
    # msg.html = html_body
    thr = Thread(target=sendAsyncEmail, args=[msg])
    thr.start()
    # mail.send(msg)
