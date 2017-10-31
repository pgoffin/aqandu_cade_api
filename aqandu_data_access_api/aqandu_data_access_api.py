# import os
import time
import requests
import sys

from datetime import datetime
from flask import Flask, jsonify, request
from influxdb import InfluxDBClient
from flask_compress import Compress
from flask_cors import CORS

app = Flask(__name__)   # create the application instance :)
app.config.from_object('config.py')
app.config.from_pyfile('config.py')


influxCLient = InfluxDBClient(
        host=app.config['INFLUX_HOST'],
        port=app.config['INFLUX_PORT'],
        username=app.config['INFLUX_USERNAME'],
        password=app.config['INFLUX_PASSWORD'],
        database=app.config['INFLUX_POLLING_DATABASE    '],
        ssl=app.config['SSL'],
        verify_ssl=app.config['SSL'])


Compress(app)
CORS(app)
# sentry = Sentry(app, dsn=config['sentry'])


@app.route('/api/v1/sensorsShorter', methods=['GET'])
def getAllSensors():
    query = "SELECT MEAN(\"pm2.5 (ug/m^3)\") FROM airQuality WHERE time >= '2017-09-06T00:00:00Z' " \
            "GROUP BY ID, Latitude, Longitude " \
            "LIMIT 100"

    start = time.time()
    data = influxCLient.query(query, epoch='ms')
    data = data.raw
    end = time.time()

    print("*********** Time to download:", end - start)
    # print(data)

    return jsonify(data)


@app.route('/api/v1/sensorsLonger', methods=['GET'])
def getAllSensorsLonger():

    TIMESTAMP = datetime.now().isoformat()

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
    data = influxCLient.query(query, epoch=None)
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

    print("*********** Time to download:", end - start)

    return jsonify(stations)

# get all ID tags
# for each tag check purpleAir, DAQ and mesowest for the location data


@app.route('/api/v1/dataFrom', methods=['GET'])
# def getDataFrom(sensorID, startDate, endDate):
def getDataFrom():

    queryParameters= request.args
    print(queryParameters)

    # TODO do some parameter checking

    query = "SELECT \"pm2.5 (ug/m^3)\", ID FROM airQuality " \
            "WHERE ID = '" + queryParameters['id'] + "' " \
            "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

    start = time.time()

    data = influxCLient.query(query, epoch=None)
    data = data.raw

    # parse the data
    theValues = data['series'][0]['values']
    pmTimeSeries = list(map(lambda x: {'time': x[0], 'pm25': x[1]}, theValues))

    # print(pmTimeSeries)

    end = time.time()

    print("*********** Time to download:", end - start)

    return jsonify(pmTimeSeries)

# # if __name__ == "__main__":
#     app.run(debug=True)
