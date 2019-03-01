import time
# import distutils
from threading import Thread

from datetime import datetime
from distutils.util import strtobool
from flask import jsonify, request, Blueprint
from flask import current_app
from flask_mail import Message
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from pymongo import MongoClient
from twilio.rest import Client
# from werkzeug.local import LocalProxy

import logging

from aqandu_data_access_api_py3 import mail, app

# LOGGER = LocalProxy(lambda: current_app.logger)
LOGGER = logging.getLogger('aqandu')

mongo = Blueprint('mongo', __name__)


# http://air.eng.utah.edu/dbapi/api/sensorIsConnected?sensor_mac=F4:5E:AB:9C:02:DF&email=pink@sci.com&phone=+8015583223&mapVisibility=true
# http://air.eng.utah.edu/dbapi/api/registerSensor
@mongo.route('/api/registerSensor', methods=['POST'])
def registerSensor():

    LOGGER.info('*********** registerSensor POST request started ***********')

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    influxClientLoggingSensorConnections = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                                          port=current_app.config['INFLUX_PORT'],
                                                          username=current_app.config['INFLUX_USERNAME'],
                                                          password=current_app.config['INFLUX_PASSWORD'],
                                                          database=current_app.config['INFLUX_AIRU_LOGGING_SENSOR_CONNECTION_DATABASE'],
                                                          ssl=current_app.config['SSL'],
                                                          verify_ssl=current_app.config['SSL'])

    # # TWILIO client
    client = Client(current_app.config['TWILIO_ACCOUNT_SID'], current_app.config['TWILIO_AUTH_TOKEN'])

    queryParameters = request.get_json()
    LOGGER.info(queryParameters)

    macAddress = queryParameters['sensor_mac']
    email = queryParameters['sensor_holder']
    # phone = queryParameters['phone']
    mapVisibility = queryParameters['mapVisibility']
    # casting to bool
    mapVisibility_bool = bool(strtobool(mapVisibility))

    # TODO Do parameter checking
    # TODO check if the MAC address is in our list of MAC addresses

    try:
        now = datetime.utcnow()

        phoneNumber = queryParameters['phone']
        if queryParameters['phone'] != '':
            phoneNumber = '+1' + queryParameters['phone']

        aSensor = {"macAddress": macAddress,
                   "email": email,
                   "phone": phoneNumber,             # set for consistency with further deployments
                   "mapVisibility": mapVisibility_bool,   # set for consistency with further deployments
                   "createdAt": now}

        sensorConnectionMeasurement = {
            'measurement': current_app.config['INFLUX_AIRU_LOGGING_SENSOR_MEASUREMENT'],
            'fields': {
                'email': email,
                'mapVisibility': mapVisibility_bool,   # set for consistency with further deployments
                'phone': phoneNumber  # set for consistency with further deployments
            },
            'tags': {
                'macAddress': macAddress
            }
        }

        LOGGER.info(sensorConnectionMeasurement)
        startInfluxWrite = time.time()

        # logging every connection attempt
        influxClientLoggingSensorConnections.write_points([sensorConnectionMeasurement])

        endInfluxWrite = time.time()
        timeToWriteInflux = endInfluxWrite - startInfluxWrite
        LOGGER.info('*********** Time to write to influx: %s', timeToWriteInflux)

        # check if already entry with given MAC address if no insert, if yes more checks
        startMongoWrite = time.time()
        if not macAddress:
            entryWithGivenMAC = None
        else:
            entryWithGivenMAC = db.sensors.find_one({'macAddress': macAddress})

        LOGGER.info(entryWithGivenMAC)
        if entryWithGivenMAC is None:
            db.sensors.insert_one(aSensor)
            LOGGER.info('%s inserted into Mongo db.', macAddress)
        else:
            # a mac address will always have only one entry, if there is already an entry replace it with the new entry
            db.sensors.replace_one({'_id': entryWithGivenMAC['_id']}, aSensor)
            LOGGER.info('%s was already present. Replaced with new information.', macAddress)

        endMongoWrite = time.time()
        timeToWriteMongo = endMongoWrite - startMongoWrite
        LOGGER.info('*********** Time to write to Mongo: %s', timeToWriteMongo)

        #  if there is a phone number prefer phone
        # theMessage = 'Hello from AQandU! Your sensor with MAC address ' + macAddress + ' is now connected to the internet and is gathering data. Thank you for participating!'
        theMessage = 'Hello from AQandU! Your sensor is now connected to the internet and is gathering data. Thank you for participating! AQandU Team'

        if phoneNumber != '':
            LOGGER.info('sending a text to ' + phoneNumber)
            startSendText = time.time()

            sender = current_app.config['PHONE_NUMBER_TO_SEND_MESSAGE']
            recipient = phoneNumber

            sendText(client, sender, recipient, theMessage)

            endSendText = time.time()
            timeToSendText = endSendText - startSendText
            LOGGER.info('*********** Time to send text: %s', timeToSendText)
        else:
            LOGGER.info('no phone number provided')

        if email != '':

            LOGGER.info('sending an email to ' + email)
            startSendEmail = time.time()

            aSubject = 'AQandU sensor is connected'
            recipients = [email]

            sendEmail(aSubject, recipients, theMessage)

            endSendEmail = time.time()
            timeToSendEmail = endSendEmail - startSendEmail
            LOGGER.info('*********** Time to send Email: %s', timeToSendEmail)

        else:
            LOGGER.info('no email address provided')

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
