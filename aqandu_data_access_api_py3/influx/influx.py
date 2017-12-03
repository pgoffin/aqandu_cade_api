
import requests
import pprint
import sys
import time

from datetime import datetime, timedelta
from flask import jsonify, request, Blueprint
from influxdb import InfluxDBClient
from pymongo import MongoClient
from werkzeug.local import LocalProxy

# from .. import app
from flask import current_app


influx = Blueprint('influx', __name__)
logger = LocalProxy(lambda: current_app.logger)

# lookup table to transform querString to influx column name
lookupQueryParameterToInflux = {
    'pm25': '\"pm2.5 (ug/m^3)\"',
    'altitude': '\"Altitude (m)\"',
    'humidity': '\"Humidity (%)\"',
    'id': 'ID',
    'latitude': 'Latitude',
    'longitude': 'Longitude',
    'ozon': '\"Ozon concentration (ppb)\"',
    'pressure': '\"Pressure (Pa)\"',
    'sensor_model': '\"Sensor Model\"',
    'sensor_source': '\"Sensor Source\"',
    'sensor_version': '\"Sensor Version\"',
    'sensor_error': '\"Sensor error code\"',
    'solar_radiation': '\"Solar radiation (W/m**2)\"',
    'start': 'Start',
    'temp': '\"Temp (*C)\"',
    'wind_direction': '\"Wind direction (compass degree)\"',
    'wind_gust': '\"Wind gust (m/s)\"',
    'wind_speed': '\"Wind speed (m/s)\"',
    'pm1': '\"pm1.0 (ug/m^3)\"',
    'pm10': '\"pm10.0 (ug/m^3)\"'
}


@influx.route('/api/liveSensors', methods=['GET'])
def getLiveSensors():
    """Get sensors that are active (pushed data) since yesterday (beginning of day)"""

    logger.info('liveSensors request started')

    now = datetime.now()
    yesterday = now - timedelta(days=1)

    yesterdayBeginningOfDay = yesterday.replace(hour=00, minute=00, second=00)
    yesterdayStr = yesterdayBeginningOfDay.strftime('%Y-%m-%dT%H:%M:%SZ')

    influxClient = InfluxDBClient(
            host=current_app.config['INFLUX_HOST'],
            port=current_app.config['INFLUX_PORT'],
            username=current_app.config['INFLUX_USERNAME'],
            password=current_app.config['INFLUX_PASSWORD'],
            database=current_app.config['INFLUX_POLLING_DATABASE'],
            ssl=current_app.config['SSL'],
            verify_ssl=current_app.config['SSL'])

    queryInflux = "SELECT ID, \"Sensor Source\", Latitude, Longitude, LAST(\"pm2.5 (ug/m^3)\") AS pm25, \"Sensor Model\" " \
            "FROM airQuality WHERE time >= '" + yesterdayStr + "' " \
            "GROUP BY ID, Latitude, Longitude, \"Sensor Source\"" \
            "LIMIT 400"

    start = time.time()
    data = influxClient.query(queryInflux, epoch='ms')
    data = data.raw

    liveAirUs = getAllCurrentlyLiveAirUs()
    print(liveAirUs)


    dataSeries = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), data['series']))

    end = time.time()

    print("*********** Time to download:", end - start)

    return jsonify(dataSeries)


@influx.route('/api/sensorsLonger', methods=['GET'])
def getAllSensorsLonger():

    logger.info('sensorsLonger request started')

    TIMESTAMP = datetime.now().isoformat()

    influxClient = InfluxDBClient(
            host=current_app.config['INFLUX_HOST'],
            port=current_app.config['INFLUX_PORT'],
            username=current_app.config['INFLUX_USERNAME'],
            password=current_app.config['INFLUX_PASSWORD'],
            database=current_app.config['INFLUX_POLLING_DATABASE'],
            ssl=current_app.config['SSL'],
            verify_ssl=current_app.config['SSL'])

    stations = {}

    # DAQ
    DAQ_SITES = [{
        'ID': 'Rose Park',
        'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=rp',
        'lat': 40.7955,
        'lon': -111.9309,
        'elevation': 1295,
    }, {
        'ID': 'Hawthorne',
        'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=slc',
        'lat': 40.7343,
        'lon': -111.8721,
        'elevation': 1306
    }, {
        'ID': 'Herriman',
        'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=h3',
        'lat': 40.496408,
        'lon': -112.036305,
        'elevation': 1534
    }, {
        'ID': 'Bountiful',
        'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=bv',
        'lat': 40.903,
        'lon': -111.8845,
        'elevation': None
    }, {
        'ID': 'Magna (Met only)',
        'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=mg',
        'lat': 40.7068,
        'lon': -112.0947,
        'elevation': None
    }]

    start = time.time()

    for aStation in DAQ_SITES:
        stations[str(aStation['ID'])] = {'ID': aStation['ID'], 'Latitude': aStation['lat'], 'Longitude': aStation['lon'], 'elevation': aStation['elevation']}

    # PURPLE AIR
    try:
        purpleAirData = requests.get("https://map.purpleair.org/json")
        purpleAirData.raise_for_status()
    except requests.exceptions.HTTPError as e:
        sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.Timeout as e:
        sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.TooManyRedirects as e:
        sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.RequestException as e:
        sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
        return []

    purpleAirData = purpleAirData.json()['results']

    for aStation in purpleAirData:
        stations[str(aStation['ID'])] = {'ID': aStation['ID'], 'Latitude': aStation['Lat'], 'Longitude': aStation['Lon'],  'elevation': None}

    # MESOWEST
    mesowestURL = 'http://api.mesowest.net/v2/stations/timeseries?recent=15&token=demotoken&stid=mtmet,wbb,NAA,MSI01,UFD10,UFD11&vars=PM_25_concentration'

    try:
        mesowestData = requests.get(mesowestURL)
        mesowestData.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # statusCode = e.response.status_code
        sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.Timeout as e:
        # Maybe set up for a retry, or continue in a retry loop
        sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.TooManyRedirects as e:
        # Tell the user their URL was bad and try a different one
        sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
        return []
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
        return []

    mesowestData = mesowestData.json()['STATION']

    for aStation in mesowestData:
        stations[str(aStation['STID'])] = {'ID': aStation['STID'], 'Latitude': aStation['LATITUDE'], 'Longitude': aStation['LONGITUDE'],  'elevation': aStation['ELEVATION']}

    end = time.time()

    query = "SHOW TAG VALUES from airQuality WITH KEY = ID"
    data = influxClient.query(query, epoch=None)
    data = data.raw

    theValues = data['series'][0]['values']
    allIDs = list(map(lambda x: str(x[1]), theValues))
    print(stations)
    print(allIDs)

    stationstoBeShowed = []
    for anID in allIDs:
        sensorAvailable = stations.get(anID)

        if sensorAvailable is not None:
            stationstoBeShowed.append(sensorAvailable)

    print(stationstoBeShowed)

    print("*********** Time to download:", end - start, '***********')

    return jsonify(stations)

# get all ID tags
# for each tag check purpleAir, DAQ and mesowest for the location data


# /api/rawDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=all
# /api/rawDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=pm25,pm1
@influx.route('/api/rawDataFrom', methods=['GET'])
def getRawDataFrom():

    logger.info('rawDataFrom request started')

    influxClient = InfluxDBClient(
            host=current_app.config['INFLUX_HOST'],
            port=current_app.config['INFLUX_PORT'],
            username=current_app.config['INFLUX_USERNAME'],
            password=current_app.config['INFLUX_PASSWORD'],
            database=current_app.config['INFLUX_POLLING_DATABASE'],
            ssl=current_app.config['SSL'],
            verify_ssl=current_app.config['SSL'])

    queryParameters = request.args
    print(queryParameters)
    # jsonParameters = request.get_json(force=True)
    # print('jsonParameters', jsonParameters)

    # TODO do some parameter checking
    # TODO check if queryParameters exist if not write excpetion

    selectString = createSelection('raw', queryParameters)

    query = "SELECT " + selectString + " FROM airQuality " \
            "WHERE ID = '" + queryParameters['id'] + "' " \
            "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

    start = time.time()

    data = influxClient.query(query, epoch=None)
    data = data.raw

    theValues = data['series'][0]['values']
    theColumns = data['series'][0]['columns']

    # pmTimeSeries = list(map(lambda x: {'time': x[0], 'pm25': x[1]}, theValues))
    dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))

    end = time.time()

    print("*********** Time to download:", end - start, '***********')

    return jsonify(dataSeries)


# http://0.0.0.0:5000/api/processedDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&function=mean&functionArg=pm25&timeInterval=30m
@influx.route('/api/processedDataFrom', methods=['GET'])
def getProcessedDataFrom():

    logger.info('processedDataFrom request started')

    influxClient = InfluxDBClient(
            host=current_app.config['INFLUX_HOST'],
            port=current_app.config['INFLUX_PORT'],
            username=current_app.config['INFLUX_USERNAME'],
            password=current_app.config['INFLUX_PASSWORD'],
            database=current_app.config['INFLUX_POLLING_DATABASE'],
            ssl=current_app.config['SSL'],
            verify_ssl=current_app.config['SSL'])

    queryParameters = request.args
    print(queryParameters)

    # TODO do some parameter checking
    # TODO check if queryParameters exist if not write excpetion

    selectString = createSelection('processed', queryParameters)

    query = "SELECT " + selectString + " FROM airQuality " \
            "WHERE ID = '" + queryParameters['id'] + "' " \
            "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' GROUP BY time(" + queryParameters['timeInterval'] + ")"

    start = time.time()

    data = influxClient.query(query, epoch=None)
    data = data.raw

    # parse the data
    theValues = data['series'][0]['values']
    pmTimeSeries = list(map(lambda x: {time: x[0], 'pm2.5 (ug/m^3)': x[1]}, theValues))

    # print(pmTimeSeries)

    end = time.time()

    print("*********** Time to download:", end - start, '***********')

    return jsonify(pmTimeSeries)


# HELPER FUNCTIONS

def createSelection(typeOfQuery, querystring):
    """Creates the selection string for the SELECT statement."""

    if typeOfQuery == 'raw':

        whatToShow = querystring['show'].split(',')

        # create the selection string
        if 'all' in whatToShow:
            selectString = "*"
        else:
            selectString = 'ID'
            for show in whatToShow:
                showExists = lookupQueryParameterToInflux.get(show)

                if show != 'id' and showExists is not None:
                    selectString = selectString + ", " + showExists

    elif typeOfQuery == 'processed':
        argument = querystring['functionArg']
        argumentExists = lookupQueryParameterToInflux.get(argument)

        if argumentExists is not None:
            selectString = querystring['function'] + "(" + argumentExists + ")"

    return selectString


def getAllCurrentlyLiveAirUs():

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    print(db)
    liveAirUs = []
    print(db.sensors.count())

    cursor = db.sensors.find({})
    print(cursor)
    for doc in cursor:
        print(doc)
        logger.info(doc)
    #     liveAirUs.append({'mac': aSensor['sensor_mac'], 'registeredAt': aSensor['created_at']})

    return liveAirUs
