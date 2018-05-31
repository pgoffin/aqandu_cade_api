# import requests
# import sys
# import bson
# import pytz
import math
import time

from datetime import datetime, timedelta
from flask import jsonify, request, Blueprint, redirect, render_template, url_for, make_response
from influxdb import InfluxDBClient, DataFrameClient
from pymongo import MongoClient
from werkzeug.local import LocalProxy
import pandas as pd

# from .. import app
from flask import current_app


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class UnknownIDError(Error):
    """Raised when ID is not in db"""
    pass


influx = Blueprint('influx', __name__, template_folder='templates')
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
    'temperature': '\"Temp (*C)\"',
    'wind_direction': '\"Wind direction (compass degree)\"',
    'wind_gust': '\"Wind gust (m/s)\"',
    'wind_speed': '\"Wind speed (m/s)\"',
    'pm1': '\"pm1.0 (ug/m^3)\"',
    'pm10': '\"pm10.0 (ug/m^3)\"',
    'posix': 'POSIX',
    'secActive': 'SecActive',
    'co': 'CO'
}

# keys are also measurement names, values are the respective field key
lookupParameterToAirUInflux = {
    'altitude': 'Altitude',
    'humidity': 'Humidity',
    'latitude': 'Latitude',
    'longitude': 'Longitude',
    'pm1': 'PM1',
    'pm10': 'PM10',
    'pm25': '\"PM2.5\"',
    'temperature': 'Temperature',
    'posix': 'POSIX',
    'secActive': 'SecActive',
    'errors': 'Errors',
    'co': 'CO'
}


@influx.route("/api/dashboard")
def dashboard():

    logger.info('********** Dashboard **********')

    # Server won't break if there's a render issue
    try:
        return render_template('dashboard.html')
    except Exception as e:
        logger.info('dashboard.html could not be rendered')
        return str(e)


@influx.route("/api/errorHandler/<error>")
def errorHandler(error):

    logger.info('********** errorHandler **********')
    logger.info('errorHandler with error={}'.format(str(error)))

    try:
        return render_template('error_template.html', error=error)
    except Exception as e:
        logger.info('error_template.html could not be rendered')
        return str(e)


@influx.route("/api/get_data", methods=['POST'])
def get_data():

    logger.info('********** download_file **********')

    dataType = request.form['measType']
    sensorList = request.form['sensorIDs']
    startDate = request.form['startDate']
    endDate = request.form['endDate']

    logger.info(dataType)
    logger.info(sensorList)
    logger.info(startDate)
    logger.info(endDate)

    logger.info('dataType={}, sensorList={}, startDate={}, endDate={}'.format(dataType, sensorList, startDate, endDate))
    if dataType == 'Not Supported':
        msg = "Option is not supported"
        return redirect(url_for("errorHandler", error=msg))

    # Format the dates to the correct Influx string
    try:
        start_dt = datetime.strptime(startDate, '%Y-%m-%d')
        end_dt = datetime.strptime(endDate, '%Y-%m-%d')
    except ValueError as e:
        logger.info('date conversion error: {}'.format(str(e)))
        return redirect(url_for("errorHandler", error='ERROR: ' + str(e)))

    start_influx_query = datetime.strftime(start_dt, '%Y-%m-%dT%H:%M:%SZ')
    end_influx_query = datetime.strftime(end_dt, '%Y-%m-%dT%H:%M:%SZ')

    # This dataframe will hold data for all queried sensors
    dff = pd.DataFrame()

    # Sanitize the sensor list input
    sensorList = sensorList.replace(' ', '')
    sensorList = sensorList.replace('RosePark', 'Rose Park')
    sensorList = sensorList.split(',')
    sensorList = [s.strip() for s in sensorList]
    sensorList = sort_alphanum(sensorList)

    logger.info('Sanitized sensorList: {}'.format(sensorList))

    airU_in_list = [getSensorSource(s) == 'AirU' for s in sensorList]

    if dataType != 'pm25' and not all(airU_in_list):
        logger.info('unexpected data type for sensor list - redirect to errorHandler')
        return redirect(url_for("errorHandler", error='You cannot access that data type for sensors that are not AirU sensors'))

    customIDToMAC = None
    if any(airU_in_list):
        logger.info('{} AirU sensors in the list. Getting sensor ID to mac dict now.'.format(airU_in_list.count(True)))

        # getting the mac address from the customID send as a query parameter
        customIDToMAC = getCustomSensorIDToMAC()

    DFclient = None
    for sensor in sensorList:

        sensorSource = getSensorSource(sensor)

        logger.info('sensor={}, source={}'.format(sensor, sensorSource))

        if not sensorSource:
            logger.info('Could not find the sensor source for {}, going to the next sensor'.format(sensor))
            continue

        elif sensorSource == 'AirU':

            try:
                ID = customIDToMAC[sensor]
            except ValueError as e:
                logger.info('{} is an unknown ID, not in DB. Error: {}'.format(sensor, str(e)))

            database = 'INFLUX_AIRU_DATABASE'
            measName = dataType
            try:
                influx_fieldKey = lookupParameterToAirUInflux[dataType]
            except KeyError as e:
                return str(e)

        else:
            ID = sensor     # Sensors and ID's have the same name
            database = 'INFLUX_POLLING_DATABASE'
            measName = 'airQuality'  # All fields are in the airQuality measurement table
            try:
                influx_fieldKey = lookupQueryParameterToInflux[dataType]
            except KeyError as e:
                return str(e)

        # strip the double quote (which was needed for query)
        dataframe_key = influx_fieldKey.replace('"', '')

        # Initial setup (only done once)
        if DFclient is None:
            DFclient = DataFrameClient(host=current_app.config['INFLUX_HOST'],
                                       port=current_app.config['INFLUX_PORT'],
                                       username=current_app.config['INFLUX_USERNAME'],
                                       password=current_app.config['INFLUX_PASSWORD'],
                                       database=current_app.config[database],
                                       ssl=current_app.config['SSL'],
                                       verify_ssl=current_app.config['SSL'])

        # Switch databases if necessary (IDs are sorted, so this will be minimal)
        elif DFclient._database != current_app.config[database]:
            DFclient.switch_database(database=current_app.config[database])

        query = "SELECT * FROM {} WHERE ID='{}' AND time >= '{}' AND time <= '{}'".format(measName, ID,
                                                                                          start_influx_query,
                                                                                          end_influx_query)

        df_dict = DFclient.query(query, chunked=True)

        if not df_dict:
            print('\n{} has no data for the given timeframe\n'.format(sensor))
            dff[sensor] = ''    # Fill column with NaN

        else:
            df = df_dict[measName]
            dff = AddSeries2DataFrame(dff, df, sensor, dataframe_key)

    dff.index.name = 'time'

    # Return the csv file if it isn't empty
    if not dff.empty:
        name_or_multiple = '_' + sensorList[0] if len(sensorList) == 1 else '_multiple'
        filename = 'AirU{}_{}_{}_{}.csv'.format(name_or_multiple, dataframe_key, startDate, endDate)
        response = make_response(dff.to_csv())      # doesn't save a copy locally
        cd = 'attachment; filename={}'.format(filename)
        response.headers['Content-Disposition'] = cd
        response.mimetype = 'text/csv'
        # flash('Data was successfully downloaded.')
        return response     # page is automatically reloaded after response is sent

    # Otherwise notify the user that the data doesn't exist
    else:
        # flash('Data does not exist.')
        return redirect(url_for("influx.dashboard"))


@influx.route('/api/liveSensors/<type>', methods=['GET'])
def getLiveSensors(type):
    """Get sensors that are active (pushed data) since yesterday (beginning of day)"""

    logger.info('*********** liveSensors request started ***********')

    # now = datetime.now()
    #
    # localTimezone = pytz.timezone('MST')
    # local_dt = localTimezone.localize(now, is_dst=None)  # now local time on server is MST, add that information to the time
    # utc_dt = local_dt.astimezone(pytz.utc)  # trasnform local now time to UTC
    utc_dt = datetime.utcnow()

    yesterday = utc_dt - timedelta(days=1)
    nowMinus5 = utc_dt - timedelta(minutes=5)

    # yesterdayBeginningOfDay = yesterday.replace(hour=00, minute=00, second=00)
    # yesterdayStr = yesterdayBeginningOfDay.strftime('%Y-%m-%dT%H:%M:%SZ')
    yesterdayStr = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(yesterdayStr)
    nowMinus5Str = nowMinus5.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(nowMinus5Str)

    dataSeries = []
    start = time.time()

    if type == 'purpleAir':

        dataSeries = getInfluxPollingSensors(yesterdayStr)

    elif type == 'airU':

        # dataSeries = getInfluxAirUSensors(yesterdayStr, nowMinus5Str)
        dataSeries = getInfluxAirUSensors(nowMinus5Str)

    elif type == 'all':

        logger.info('get all dataSeries started')

        pollingDataSeries = getInfluxPollingSensors(yesterdayStr)
        logger.info(pollingDataSeries)

        # airUDataSeries = getInfluxAirUSensors(yesterdayStr, nowMinus5Str)
        airUDataSeries = getInfluxAirUSensors(nowMinus5Str)
        logger.info(airUDataSeries)

        dataSeries = pollingDataSeries + airUDataSeries
        logger.info(dataSeries)

        logger.info('get all dataSeries done')

    end = time.time()

    print("*********** Time to download:", end - start)

    return jsonify(dataSeries)


# @influx.route('/api/sensorsLonger', methods=['GET'])
# def getAllSensorsLonger():
#
#     logger.info('sensorsLonger request started')
#
#     TIMESTAMP = datetime.now().isoformat()
#
#     influxClientPolling = InfluxDBClient(
#                 host=current_app.config['INFLUX_HOST'],
#                 port=current_app.config['INFLUX_PORT'],
#                 username=current_app.config['INFLUX_USERNAME'],
#                 password=current_app.config['INFLUX_PASSWORD'],
#                 database=current_app.config['INFLUX_POLLING_DATABASE'],
#                 ssl=current_app.config['SSL'],
#                 verify_ssl=current_app.config['SSL'])
#
#     stations = {}
#
#     # DAQ
#     DAQ_SITES = [{
#         'ID': 'Rose Park',
#         'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=rp',
#         'lat': 40.7955,
#         'lon': -111.9309,
#         'elevation': 1295,
#     }, {
#         'ID': 'Hawthorne',
#         'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=slc',
#         'lat': 40.7343,
#         'lon': -111.8721,
#         'elevation': 1306
#     }, {
#         'ID': 'Herriman',
#         'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=h3',
#         'lat': 40.496408,
#         'lon': -112.036305,
#         'elevation': 1534
#     }, {
#         'ID': 'Bountiful',
#         'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=bv',
#         'lat': 40.903,
#         'lon': -111.8845,
#         'elevation': None
#     }, {
#         'ID': 'Magna (Met only)',
#         'dataFeed': 'http://air.utah.gov/xmlFeed.php?id=mg',
#         'lat': 40.7068,
#         'lon': -112.0947,
#         'elevation': None
#     }]
#
#     start = time.time()
#
#     for aStation in DAQ_SITES:
#         stations[str(aStation['ID'])] = {'ID': aStation['ID'], 'Latitude': aStation['lat'], 'Longitude': aStation['lon'], 'elevation': aStation['elevation']}
#
#     # PURPLE AIR
#     try:
#         purpleAirData = requests.get("https://map.purpleair.org/json")
#         purpleAirData.raise_for_status()
#     except requests.exceptions.HTTPError as e:
#         sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.Timeout as e:
#         sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.TooManyRedirects as e:
#         sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.RequestException as e:
#         sys.stderr.write('%s\tProblem acquiring PurpleAir data (https://map.purpleair.org/json);\t%s.\n' % (TIMESTAMP, e))
#         return []
#
#     purpleAirData = purpleAirData.json()['results']
#
#     for aStation in purpleAirData:
#         stations[str(aStation['ID'])] = {'ID': aStation['ID'], 'Latitude': aStation['Lat'], 'Longitude': aStation['Lon'],  'elevation': None}
#
#     # MESOWEST
#     mesowestURL = 'http://api.mesowest.net/v2/stations/timeseries?recent=15&token=demotoken&stid=mtmet,wbb,NAA,MSI01,UFD10,UFD11&vars=PM_25_concentration'
#
#     try:
#         mesowestData = requests.get(mesowestURL)
#         mesowestData.raise_for_status()
#     except requests.exceptions.HTTPError as e:
#         # statusCode = e.response.status_code
#         sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.Timeout as e:
#         # Maybe set up for a retry, or continue in a retry loop
#         sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.TooManyRedirects as e:
#         # Tell the user their URL was bad and try a different one
#         sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
#         return []
#     except requests.exceptions.RequestException as e:
#         # catastrophic error. bail.
#         sys.stderr.write('%s\tProblem acquiring Mesowest data;\t%s.\n' % (TIMESTAMP, e))
#         return []
#
#     mesowestData = mesowestData.json()['STATION']
#
#     for aStation in mesowestData:
#         stations[str(aStation['STID'])] = {'ID': aStation['STID'], 'Latitude': aStation['LATITUDE'], 'Longitude': aStation['LONGITUDE'],  'elevation': aStation['ELEVATION']}
#
#     end = time.time()
#
#     query = "SHOW TAG VALUES from airQuality WITH KEY = ID"
#     data = influxClientPolling.query(query, epoch=None)
#     data = data.raw
#
#     theValues = data['series'][0]['values']
#     allIDs = list(map(lambda x: str(x[1]), theValues))
#     print(stations)
#     print(allIDs)
#
#     stationstoBeShowed = []
#     for anID in allIDs:
#         sensorAvailable = stations.get(anID)
#
#         if sensorAvailable is not None:
#             stationstoBeShowed.append(sensorAvailable)
#
#     print(stationstoBeShowed)
#
#     print("*********** Time to download:", end - start, '***********')
#
#     return jsonify(stations)

# get all ID tags
# for each tag check purpleAir, DAQ and mesowest for the location data


# /api/rawDataFrom?id=1010&sensorSource=PurpleAir&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=all
# /api/rawDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=pm25,pm1
@influx.route('/api/rawDataFrom', methods=['GET'])
def getRawDataFrom():

    airUdbs = ['altitude', 'humidity', 'latitude', 'longitude', 'pm1', 'pm25', 'pm10', 'posix', 'secActive', 'temperature', 'co']

    logger.info('*********** rawDataFrom request started ***********')

    queryParameters = request.args
    logger.info(queryParameters)

    dataSeries = []
    if queryParameters['sensorSource'] == 'airu':
        logger.info('airu')
        logger.info(queryParameters['sensorSource'])

        start = time.time()

        # create createSelection
        whatToShow = queryParameters['show'].split(',')
        logger.info(whatToShow)

        # http://0.0.0.0:5000/api/rawDataFrom?id=D0B5C2F31E1F&sensorSource=AirU&start=2017-12-02T22:17:00Z&end=2017-12-03T22:17:00Z&show=all

        influxClientAirU = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                          port=current_app.config['INFLUX_PORT'],
                                          username=current_app.config['INFLUX_USERNAME'],
                                          password=current_app.config['INFLUX_PASSWORD'],
                                          database=current_app.config['INFLUX_AIRU_DATABASE'],
                                          ssl=current_app.config['SSL'],
                                          verify_ssl=current_app.config['SSL'])

        # getting the mac address from the customID send as a query parameter
        customIDToMAC = getCustomSensorIDToMAC()

        theID = queryParameters['id']
        if theID in customIDToMAC:
            theID = customIDToMAC[theID]
        else:
            logger.info('this is an unknow ID, not in db')
            message = {'status': 404,
                       'message': 'unknown ID, so sensor with that ID'
                       }
            errorResp = jsonify(message)
            errorResp.headers.add('Access-Control-Allow-Origin', '*')
            errorResp.status_code = 404

            return errorResp
            # raise UnknownIDError("unknown ID")

        # query each db
        toShow = []
        if 'all' in whatToShow:
            toShow = airUdbs
        else:
            toShow = whatToShow

        # loop over the measurements
        for aDB in toShow:

            if aDB == 'pm25':
                # normalizing to pm25
                fieldString = lookupParameterToAirUInflux.get(aDB) + ' AS pm25'
            else:
                fieldString = lookupParameterToAirUInflux.get(aDB)

            queryAirU = "SELECT " + fieldString + " FROM " + aDB + " " \
                        "WHERE ID = '" + theID + "' " \
                        "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

            logger.info(queryAirU)

            dataAirU = influxClientAirU.query(queryAirU, epoch=None)
            dataAirU = dataAirU.raw

            # check if query gave data back
            if 'series' in dataAirU:
                valuesAirU = dataAirU['series'][0]['values']
                columnsAirU = dataAirU['series'][0]['columns']

                if not dataSeries:
                    dataSeries = list(map(lambda x: dict(zip(columnsAirU, x)), valuesAirU))
                else:
                    newDataSeries = list(map(lambda x: dict(zip(columnsAirU, x)), valuesAirU))

                    # print(list(zip(dataSeries, newDataSeries)))
                    # as a security I add the timestamp from the merged db, the difference in timestamps are in the 0.1 milisecond (0.0001)
                    # dataSeries = list(map(lambda y: {**y[0], **y[1], 'time_' + aDB: y[1]['time']} if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0] else {0}, list(zip(dataSeries, newDataSeries))))

                    tmpList = []
                    for dict1, dict2 in list(zip(dataSeries, newDataSeries)):
                        # print(elem1, elem2)
                        if dict1['time'].split('.')[0] == dict2['time'].split('.')[0]:
                            # replace the time attribute with a new key so it does not copy over the dict1's time when being merged
                            dict2['time_' + aDB] = dict2.pop('time')
                            mergedObject = mergeTwoDicts(dict1, dict2)

                            tmpList.append(mergedObject)

                    dataSeries = tmpList

                # dataSeries = [{y[0], y[1]} for elem in list(zip(dataSeries, newDataSeries)) if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0]]

        queryForTags = "SELECT LAST(" + lookupParameterToAirUInflux.get("pm25") + "), ID, \"Sensor Model\" FROM pm25 " \
                       "WHERE ID = '" + theID + "' "
        logger.info(queryForTags)

        dataTags = influxClientAirU.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        logger.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        dataSeries_Tags[0]['Sensor Source'] = 'airu'
        logger.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags

        end = time.time()

    else:

        influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                             port=current_app.config['INFLUX_PORT'],
                                             username=current_app.config['INFLUX_USERNAME'],
                                             password=current_app.config['INFLUX_PASSWORD'],
                                             database=current_app.config['INFLUX_POLLING_DATABASE'],
                                             ssl=current_app.config['SSL'],
                                             verify_ssl=current_app.config['SSL'])

        # TODO do some parameter checking
        # TODO check if queryParameters exist if not write excpetion

        selectString = createSelection('raw', queryParameters)
        logger.info(selectString)

        query = "SELECT " + selectString + " FROM airQuality " \
                "WHERE ID = '" + queryParameters['id'] + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "
        logger.info(query)

        start = time.time()

        data = influxClientPolling.query(query, epoch=None)
        data = data.raw

        print(data)

        theValues = data['series'][0]['values']
        theColumns = data['series'][0]['columns']

        # pmTimeSeries = list(map(lambda x: {'time': x[0], 'pm25': x[1]}, theValues))
        dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))

        queryForTags = "SELECT LAST(" + lookupQueryParameterToInflux.get("pm25") + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality " \
                       "WHERE ID = '" + queryParameters['id'] + "' "

        dataTags = influxClientPolling.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        logger.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        logger.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags

        end = time.time()

    logger.info('*********** Time to download: %s ***********', end - start)

    resp = jsonify(newDataSeries)
    resp.status_code = 200

    return resp


# http://0.0.0.0:5000/api/processedDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&function=mean&functionArg=pm25&timeInterval=30m
@influx.route('/api/processedDataFrom', methods=['GET'])
def getProcessedDataFrom():

    logger.info('*********** processedDataFrom request started ***********')

    queryParameters = request.args
    logger.info(queryParameters)

    minutesOffset = queryParameters['start'].split(':')[1] + 'm'
    logger.info(minutesOffset)

    if queryParameters['sensorSource'] == 'airu':
        logger.info('airu')
        logger.info(queryParameters['sensorSource'])

        influxClientAirU = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                          port=current_app.config['INFLUX_PORT'],
                                          username=current_app.config['INFLUX_USERNAME'],
                                          password=current_app.config['INFLUX_PASSWORD'],
                                          database=current_app.config['INFLUX_AIRU_DATABASE'],
                                          ssl=current_app.config['SSL'],
                                          verify_ssl=current_app.config['SSL'])

        # getting the mac address from the customID send as a query parameter
        customIDToMAC = getCustomSensorIDToMAC()

        theID = queryParameters['id']
        if theID in customIDToMAC:
            theID = customIDToMAC[theID]
        else:
            logger.info('this is an unknow ID, not in db')
            message = {'status': 404,
                       'message': 'unknown ID, so sensor with that ID'
                       }
            errorResp = jsonify(message)
            errorResp.headers.add('Access-Control-Allow-Origin', '*')
            errorResp.status_code = 404

            return errorResp

        logger.info('does it go on')
        selectString = createSelection('processed', queryParameters)
        logger.info(selectString)

        query = "SELECT " + selectString + " FROM pm25 " \
                "WHERE ID = '" + theID + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' GROUP BY time(" + queryParameters['timeInterval'] + ", " + minutesOffset + ")"
        logger.info(query)

        start = time.time()

        data = influxClientAirU.query(query, epoch=None)
        data = data.raw
        logger.info(data)

        # parse the data
        theValues = data['series'][0]['values']
        theColumns = data['series'][0]['columns']

        dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))
        # pmTimeSeries = list(map(lambda x: {time: x[0], 'pm2.5 (ug/m^3)': x[1]}, theValues))

        # print(pmTimeSeries)

        queryForTags = "SELECT LAST(" + lookupParameterToAirUInflux.get(queryParameters['functionArg']) + "), ID, \"Sensor Model\" FROM pm25 " \
                       " WHERE ID = '" + theID + "' "

        dataTags = influxClientAirU.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        logger.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        dataSeries_Tags[0]['Sensor Source'] = 'airu'
        logger.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags

        end = time.time()
    else:
        influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                             port=current_app.config['INFLUX_PORT'],
                                             username=current_app.config['INFLUX_USERNAME'],
                                             password=current_app.config['INFLUX_PASSWORD'],
                                             database=current_app.config['INFLUX_POLLING_DATABASE'],
                                             ssl=current_app.config['SSL'],
                                             verify_ssl=current_app.config['SSL'])

        # queryParameters = request.args
        # logger.info(queryParameters)

        # TODO do some parameter checking
        # TODO check if queryParameters exist if not write excpetion

        selectString = createSelection('processed', queryParameters)
        logger.info(selectString)

        query = "SELECT " + selectString + " FROM airQuality " \
                "WHERE ID = '" + queryParameters['id'] + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' GROUP BY time(" + queryParameters['timeInterval'] + ", " + minutesOffset + ")"
        logger.info(query)

        start = time.time()

        data = influxClientPolling.query(query, epoch=None)
        data = data.raw
        logger.info(data)

        # parse the data
        theValues = data['series'][0]['values']
        theColumns = data['series'][0]['columns']

        dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))
        # pmTimeSeries = list(map(lambda x: {time: x[0], 'pm2.5 (ug/m^3)': x[1]}, theValues))

        # print(pmTimeSeries)

        queryForTags = "SELECT LAST(" + lookupQueryParameterToInflux.get(queryParameters['functionArg']) + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality WHERE ID = '" + queryParameters['id'] + "' "

        dataTags = influxClientPolling.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        logger.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        logger.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags

        end = time.time()

    logger.info('*********** Time to download: %s ***********', end - start)

    resp = jsonify(newDataSeries)
    resp.status_code = 200

    return resp


# http://0.0.0.0:5000/api/lastValue?fieldKey=pm25
@influx.route('/api/lastValue', methods=['GET'])
def getLastValuesForLiveSensor():

    logger.info('*********** lastPM request started ***********')

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_POLLING_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    queryParameters = request.args
    logger.info(queryParameters['fieldKey'])

    queryPolling = "SELECT LAST(" + lookupQueryParameterToInflux.get(queryParameters['fieldKey']) + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality GROUP BY ID"

    data = influxClientPolling.query(queryPolling, epoch=None)
    data = data.raw

    dataSeries = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), data['series']))

    lastValueObject = {aSensor["ID"]: aSensor for aSensor in dataSeries}

    # getting the airu data
    influxClientAirU = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                      port=current_app.config['INFLUX_PORT'],
                                      username=current_app.config['INFLUX_USERNAME'],
                                      password=current_app.config['INFLUX_PASSWORD'],
                                      database=current_app.config['INFLUX_AIRU_DATABASE'],
                                      ssl=current_app.config['SSL'],
                                      verify_ssl=current_app.config['SSL'])

    queryAirU = "SELECT LAST(" + lookupParameterToAirUInflux.get(queryParameters['fieldKey']) + "), ID, \"Sensor Model\" FROM " + queryParameters['fieldKey'] + " GROUP BY ID"

    dataAirU = influxClientAirU.query(queryAirU, epoch=None)
    dataAirU = dataAirU.raw

    dataSeriesAirU = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataAirU['series']))

    macToCustomID = getMacToCustomSensorID()

    # lastValueObjectAirU = {anAirU['ID']: anAirU for anAirU in dataSeriesAirU}
    # lastValueObjectAirU = {anAirU["ID"]: anAirU for anAirU in dataSeriesAirU}

    lastValueObjectAirU = {}
    for anAirU in dataSeriesAirU:
        newID = anAirU['ID']
        if newID in macToCustomID:
            newID = macToCustomID[newID]

        anAirU['Sensor Source'] = 'airu'

        lastValueObjectAirU[newID] = anAirU

    # for key, val in lastValueObjectAirU.items():
    #
    #     newID = key
    #     if key in macToCustomID:
    #         newID = macToCustomID[key]
    #
    #     key = newID
    #     val['Sensor Source'] = 'airu'
    #     val['ID'] = newID

    allLastValues = mergeTwoDicts(lastValueObject, lastValueObjectAirU)

    logger.info('*********** lastPM request done ***********')

    return jsonify(allLastValues)


# contour API calls
@influx.route('/api/contours', methods=['GET'])
def getContours():

    logger.info('*********** getting contours request started ***********')

    queryParameters = request.args
    logger.info(queryParameters)

    startDate = queryParameters['start']
    logger.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    logger.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    logger.info('the start date')
    logger.info(startDate)
    logger.info('the end date')
    logger.info(endDate)

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    # first take estimates from high collection
    # then estimates from low collection
    allHighEstimates = db.timeSlicedEstimates_high.find().sort('estimationFor', -1)
    lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)

    contours = []

    logger.info('the allHighEstimates')
    logger.info(allHighEstimates.count())
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        logger.info(estimationDateSliceDateHigh)
        contours.append({'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})

    logger.info('the lowEstimates')
    logger.info(lowEstimates.count())
    for estimateSliceLow in lowEstimates:
        estimationDateSliceDateLow = estimateSliceLow['estimationFor']
        logger.info(estimationDateSliceDateLow)
        contours.append({'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

    # logger.info(contours)
    #
    # logger.info(jsonify(contours))

    resp = jsonify(contours)
    resp.status_code = 200

    logger.info('*********** getting contours request done ***********')

    return resp


@influx.route('/api/getLatestContour', methods=['GET'])
def getLatestContour():

    logger.info('*********** getting latest contours request started ***********')

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    # contours = {}

    cursor = db.timeSlicedEstimates_high.find().sort('estimationFor', -1).limit(1)
    logger.info(cursor)

    for doc in cursor:
        logger.info(type(doc))
        logger.info(doc['estimate'])
        logger.info(doc['contours'])
        # logger.info(jsonify(doc))

        lastContour = {'contour': doc['contours'], 'date_utc': doc['estimationFor']}

    # logger.info(contours)

    # logger.info(jsonify(contours))

    resp = jsonify(lastContour)
    resp.status_code = 200

    logger.info('*********** getting latest contours request done ***********')

    return resp


@influx.route('/api/getEstimatesForLocation', methods=['GET'])
def getEstimatesForLocation():
    # need a location and the needed timespan

    logger.info('*********** getEstimatesForLocation started ***********')

    queryParameters = request.args
    logger.info(queryParameters)

    location_lat = queryParameters['location_lat']
    location_lng = queryParameters['location_lng']
    startDate = queryParameters['start']
    # logger.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    # logger.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    logger.info('the start date')
    logger.info(startDate)
    logger.info('the end date')
    logger.info(endDate)

    # use location to get the 4 estimation data corners
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    gridInfo = db.estimationMetadata.find_one({"metadataType": current_app.config['METADATA_TYPE_HIGH_UNCERTAINTY'], "gridID": current_app.config['CURRENT_GRID_VERSION']})

    logger.info(gridInfo)

    theCorners = {}
    if gridInfo is not None:
        theGrid = gridInfo['transformedGrid']
        numberGridCells_LAT = gridInfo['numberOfGridCells']['lat']
        numberGridCells_LONG = gridInfo['numberOfGridCells']['long']

        logger.info(theGrid)
        logger.info(numberGridCells_LAT)
        logger.info(numberGridCells_LONG)

        topRightCornerIndex = str((int(numberGridCells_LAT + 1) * int(numberGridCells_LONG + 1)) - 1)
        bottomLeftCornerIndex = str(0)

        stepSizeLat = abs(theGrid[topRightCornerIndex]['lat'][0] - theGrid[bottomLeftCornerIndex]['lat'][0]) / numberGridCells_LAT
        stepSizeLong = abs(theGrid[topRightCornerIndex]['lngs'][0] - theGrid[bottomLeftCornerIndex]['lngs'][0]) / numberGridCells_LONG

        logger.info(stepSizeLat)
        logger.info(stepSizeLong)

        fourCorners_left_index_lng = math.floor((float(location_lng) - theGrid[bottomLeftCornerIndex]['lngs'][0]) / stepSizeLong)
        fourCorners_bottom_index_lat = math.floor((float(location_lat) - theGrid[bottomLeftCornerIndex]['lat'][0]) / stepSizeLat)

        logger.info(fourCorners_left_index_lng)
        logger.info(fourCorners_bottom_index_lat)

        # leftCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_left_index_x * stepSizeLong)
        # rightCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_right_index_x * stepSizeLong)
        # bottomCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_bottom_index_y * stepSizeLat)
        # topCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_top_index_y * stepSizeLat)

        leftBottomCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        leftTopCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))
        rightBottomCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        rightTopCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))

        logger.info(leftBottomCorner_index)
        logger.info(rightBottomCorner_index)
        logger.info(leftTopCorner_index)
        logger.info(rightTopCorner_index)

        leftBottomCorner_location = theGrid[leftBottomCorner_index]
        rightBottomCorner_location = theGrid[rightBottomCorner_index]
        leftTopCorner_location = theGrid[leftTopCorner_index]
        rightTopCorner_location = theGrid[rightTopCorner_index]

        logger.info(leftBottomCorner_location)
        logger.info(rightBottomCorner_location)
        logger.info(leftTopCorner_location)
        logger.info(rightTopCorner_location)

        theCorners['leftBottomCorner'] = {'lat': leftBottomCorner_location['lat'][0], 'lng': leftBottomCorner_location['lngs'][0]}
        theCorners['rightBottomCorner'] = {'lat': rightBottomCorner_location['lat'][0], 'lng': rightBottomCorner_location['lngs'][0]}
        theCorners['leftTopCorner'] = {'lat': leftTopCorner_location['lat'][0], 'lng': leftTopCorner_location['lngs'][0]}
        theCorners['rightTopCorner'] = {'lat': rightTopCorner_location['lat'][0], 'lng': rightTopCorner_location['lngs'][0]}

    else:
        logger.info('grid info is none!')

    # get the 4 corners for each timestamp between the timespan
    # take all estimates in timeSpan

    # first take estimates from high collection
    # then estimates from low collection
    allHighEstimates = db.timeSlicedEstimates_high.find().sort('estimationFor', -1)
    lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1).limit(300)

    x = float(location_lng)
    y = float(location_lat)
    x1 = leftBottomCorner_location['lngs'][0]
    x2 = rightBottomCorner_location['lngs'][0]
    y1 = leftBottomCorner_location['lat'][0]
    y2 = leftTopCorner_location['lat'][0]

    # theDates = []
    theInterpolatedValues = []
    logger.info('the allHighEstimates')
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        # theDates.append({'date': estimationDateSliceDateHigh, 'origin': 'high'})
        logger.info(estimationDateSliceDateHigh)

        # get the corner values
        leftBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftBottomCorner_index]['pm25']
        logger.debug(leftBottomCorner_pm25valueHigh)
        rightBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightBottomCorner_index]['pm25']
        logger.debug(rightBottomCorner_pm25valueHigh)
        leftTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftTopCorner_index]['pm25']
        logger.debug(leftTopCorner_pm25valueHigh)
        rightTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightTopCorner_index]['pm25']
        logger.debug(rightTopCorner_pm25valueHigh)

        Q11 = leftBottomCorner_pm25valueHigh
        Q21 = rightBottomCorner_pm25valueHigh
        Q12 = leftTopCorner_pm25valueHigh
        Q22 = rightTopCorner_pm25valueHigh

        # do bilinear interpolation using these 4 corners
        interpolatedEstimateHigh = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)
        logger.info(interpolatedEstimateHigh)

        theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateHigh, 'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})

    logger.info('the lowEstimates')
    logger.info(lowEstimates.count())
    for estimateSliceLow in lowEstimates:
        logger.info(estimateSliceLow)
        estimationDateSliceDateLow = estimateSliceLow['estimationFor']
        # theDates.append({'date': estimationDateSliceDateLow, 'origin': 'low'})
        logger.info(estimationDateSliceDateLow)

        leftBottomCorner_pm25valueLow = estimateSliceLow['estimate'][leftBottomCorner_index]['pm25']
        rightBottomCorner_pm25valueLow = estimateSliceLow['estimate'][rightBottomCorner_index]['pm25']
        leftTopCorner_pm25valueLow = estimateSliceLow['estimate'][leftTopCorner_index]['pm25']
        rightTopCorner_pm25valueLow = estimateSliceLow['estimate'][rightTopCorner_index]['pm25']

        Q11 = leftBottomCorner_pm25valueLow
        Q21 = rightBottomCorner_pm25valueLow
        Q12 = leftTopCorner_pm25valueLow
        Q22 = rightTopCorner_pm25valueLow

        interpolatedEstimateLow = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)

        theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateLow, 'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

        logger.info('done with lowEstimates')

    logger.info(theCorners)

    resp = jsonify(theInterpolatedValues)
    resp.status_code = 200

    logger.info('*********** getting latest contours request done ***********')

    return resp


# HELPER FUNCTIONS

def createSelection(typeOfQuery, querystring):
    """Creates the selection string for the SELECT statement."""
    logger.info(querystring)
    if typeOfQuery == 'raw':

        # db = querystring['sensorSource']

        show = querystring['show'].split(',')

        # if db != 'AirU':
        # create the selection string
        if 'all' in show:
            selectString = "*"
        else:
            # selectString = 'ID, \"Sensor Model\", \"Sensor Source\"'
            selectString = ''
            for aShow in show:
                showExists = lookupQueryParameterToInflux.get(aShow)

                if aShow != 'id' and showExists is not None:
                    if aShow == 'pm25':
                        showExists = showExists + ' AS pm25'

                    # selectString = selectString + ", " + showExists + ", "
                    selectString = selectString + (', ' if selectString != '' else '') + showExists

    elif typeOfQuery == 'processed':
        argument = querystring['functionArg']

        if querystring['sensorSource'] != 'airu':
            argumentExists = lookupQueryParameterToInflux.get(argument)
        else:
            argumentExists = lookupParameterToAirUInflux.get(argument)

        logger.info(argumentExists)

        if argumentExists is not None:
            alias = ''
            if argument == 'pm25':
                alias = alias + ' AS pm25 '

            selectString = querystring['function'] + "(" + argumentExists + ")" + alias

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
    liveAirUs = []

    # for aSensor in db.sensors.find():
    for aSensor in db.liveSensors.find():
        if aSensor['macAddress']:
            liveAirUs.append({'macAddress': ''.join(aSensor['macAddress'].split(':')), 'registeredAt': aSensor['createdAt']})

    return liveAirUs


def getMacToCustomSensorID():

    logger.info('******** getMacToCustomSensorID started ********')
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    macToCustomID = {}

    # for aSensor in db.sensors.find():
    for aSensor in db.macToCustomSensorID.find():
        theMAC = ''.join(aSensor['macAddress'].split(':'))
        macToCustomID[theMAC] = aSensor['customSensorID']
        logger.info(theMAC)
        logger.info(macToCustomID)

    logger.info('******** getMacToCustomSensorID done ********')
    return macToCustomID


def getCustomSensorIDToMAC():

    logger.info('******** getMacToCustomSensorIDToMac STARTED ********')
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    customIDToMAC = {}

    # for aSensor in db.sensors.find():
    for aSensor in db.macToCustomSensorID.find():
        theMAC = ''.join(aSensor['macAddress'].split(':'))
        customIDToMAC[aSensor['customSensorID']] = theMAC
        # logger.info(theMAC)
        # logger.info(customIDToMAC)

    logger.info('******** getMacToCustomSensorIDToMac DONE ********')
    return customIDToMAC


# https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
def mergeTwoDicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def getInfluxPollingSensors(aDateStr):

    logger.info('******** influx polling started ********')

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_POLLING_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    queryInflux = "SELECT ID, \"Sensor Source\", Latitude, Longitude, LAST(\"pm2.5 (ug/m^3)\") AS pm25, \"Sensor Model\" " \
                  "FROM airQuality WHERE time >= '" + aDateStr + "' " \
                  "GROUP BY ID, Latitude, Longitude, \"Sensor Source\"" \
                  "LIMIT 400"

    # start = time.time()
    data = influxClientPolling.query(queryInflux, epoch='ms')
    data = data.raw

    dataSeries = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), data['series']))
    # print(dataSeries)

    # locating the double/parallel sensors (sensor with same location) and slightly changing the second sensors location, results in both dots visible
    for i in range(len(dataSeries)):
        # logger.info('i is %s', i)
        for j in range(i + 1, len(dataSeries)):
            # logger.info('j is %s', j)
            # logger.info('dataSeries[i] is %s', dataSeries[i])
            # logger.info('dataSeries[j] is %s', dataSeries[j])
            if dataSeries[i]['ID'] != dataSeries[j]['ID']:
                if dataSeries[i]['Latitude'] == dataSeries[j]['Latitude'] and dataSeries[i]['Longitude'] == dataSeries[j]['Longitude']:
                    dataSeries[j]['Longitude'] = str(float(dataSeries[j]['Longitude']) - 0.0005)

    logger.info(dataSeries)

    logger.info('******** influx polling done ********')

    return dataSeries


def getInfluxAirUSensors(minus5min):

    logger.info('******** influx airU started ********')

    dataSeries = []

    liveAirUs = getAllCurrentlyLiveAirUs()  # call to mongodb
    logger.info(liveAirUs)

    influxClientAirU = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                      port=current_app.config['INFLUX_PORT'],
                                      username=current_app.config['INFLUX_USERNAME'],
                                      password=current_app.config['INFLUX_PASSWORD'],
                                      database=current_app.config['INFLUX_AIRU_DATABASE'],
                                      ssl=current_app.config['SSL'],
                                      verify_ssl=current_app.config['SSL'])

    macToCustomID = getMacToCustomSensorID()
    logger.info(macToCustomID)

    for airU in liveAirUs:
        logger.info(airU)

        macAddress = airU['macAddress']
        logger.info(macAddress)

        queryInfluxAirU_lat = "SELECT MEAN(Latitude) " \
                              "FROM " + current_app.config['INFLUX_AIRU_LATITUDE_MEASUREMENT'] + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + minus5min + "'"

        logger.info(queryInfluxAirU_lat)

        dataAirU_lat = influxClientAirU.query(queryInfluxAirU_lat, epoch='ms')
        dataAirU_lat = dataAirU_lat.raw
        logger.info(dataAirU_lat)

        if 'series' not in dataAirU_lat:
            logger.info('%s missing latitude', macAddress)
            continue

        avgLat = dataAirU_lat['series'][0]['values'][0][1]

        queryInfluxAirU_lng = "SELECT MEAN(Longitude) " \
                              "FROM " + current_app.config['INFLUX_AIRU_LONGITUDE_MEASUREMENT'] + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + minus5min + "' "

        logger.info(queryInfluxAirU_lng)

        dataAirU_lng = influxClientAirU.query(queryInfluxAirU_lng, epoch='ms')
        dataAirU_lng = dataAirU_lng.raw
        logger.info(dataAirU_lng)

        if 'series' not in dataAirU_lng:
            logger.info('%s missing longitude', macAddress)
            continue

        avgLng = dataAirU_lng['series'][0]['values'][0][1]

        queryInfluxAirU_lastPM25 = "SELECT LAST(" + lookupParameterToAirUInflux.get('pm25') + ") AS pm25, ID " \
                                   "FROM " + current_app.config['INFLUX_AIRU_PM25_MEASUREMENT'] + " "\
                                   "WHERE ID = '" + macAddress + "' "

        logger.info(queryInfluxAirU_lastPM25)

        dataAirU_lastPM25 = influxClientAirU.query(queryInfluxAirU_lastPM25, epoch='ms')
        dataAirU_lastPM25 = dataAirU_lastPM25.raw

        logger.info(dataAirU_lastPM25)

        if 'series' not in dataAirU_lastPM25:
            logger.info('%s missing lastPM25', macAddress)
            continue

        lastPM25 = dataAirU_lastPM25['series'][0]['values'][0][1]
        pm25time = dataAirU_lastPM25['series'][0]['values'][0][0]

        # logger.info(airU['macAddress'])

        newID = macAddress
        if macAddress in macToCustomID:
            newID = macToCustomID[macAddress]

        logger.info('newID is %s', newID)

        anAirU = {'ID': newID, 'Latitude': str(avgLat), 'Longitude': str(avgLng), 'Sensor Source': 'airu', 'pm25': lastPM25, 'time': pm25time}
        # anAirU = {'ID': airU['macAddress'], 'Latitude': str(avgLat), 'Longitude': str(avgLng), 'Sensor Source': 'airu', 'pm25': lastPM25, 'time': pm25time}
        logger.info(anAirU)

        dataSeries.append(anAirU)
        logger.info('airU appended')

    logger.info(dataSeries)
    logger.info('******** influx airU done ********')

    return dataSeries


# interpolation function
def bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2):

    logger.info('bilinearInterpolation')
    # f(x,y) = 1/((x2 - x1)(y2 - y1)) * (f(Q11) * (x2 - x)(y2 - y) + f(Q21) * (x - x1)(y2 - y) + f(Q12) * (x2 - x)(y - y1) + f(Q22) * (x - x1)(y - y1)
    logger.debug('Q11 is %s', Q11)
    logger.debug('Q12 is %s', Q12)
    logger.debug('Q21 is %s', Q21)
    logger.debug('Q22 is %s', Q22)
    logger.debug('x is %s', x)
    logger.debug('y is %s', y)
    logger.debug('y1 is %s', y1)
    logger.debug('y2 is %s', y2)
    logger.debug('x1 is %s', x1)
    logger.debug('x2 is %s', x2)

    logger.debug(type(Q11))
    logger.debug(type(Q12))
    logger.debug(type(Q21))
    logger.debug(type(Q22))
    logger.debug(type(x))
    logger.debug(type(y))
    logger.debug(type(y1))
    logger.debug(type(y2))
    logger.debug(type(x1))
    logger.debug(type(x2))

    interpolatedValue = 1.0 / ((x2 - x1) * (y2 - y1)) * ((Q11 * (x2 - x) * (y2 - y)) + (Q21 * (x - x1) * (y2 - y)) + (Q12 * (x2 - x) * (y - y1)) + (Q22 * (x - x1) * (y - y1)))

    logger.info('interpolatedValue is %s', interpolatedValue)

    return interpolatedValue


def FloorTimestamp2Minute(df):
    """
    Floor dataframe Influx Timestamps to the minute
    and turn them into a string. Then replace the
    timestamps in <df> index with these

    :param df: The Pandas DataFrame with Pandas Timestamp as index
    :return: <df> with updated index (index is string representation of timestamp with minute precision)
    """

    logger.info('********** FloorTimestamp2Minute **********')
    try:
        df.index = [t.round('T') for t in df.index.tolist()]
    except AttributeError as e:
        logger.error('FloorTimestamp2Minute: {}'.format(str(e)))
        return str(e)

    return df


def AddSeries2DataFrame(df_combined, df_single_series, sensor_name, measurement_fieldKey):
    """
    Appends a column to the existing dataframe, squishing equal timestamps to same row

    :param df_combined: the existing dataframe
        - pass an empty dataframe initially when iteratively adding columns
        - column names are <sensor_name>
        - column values are values from <measurement_fieldKey> ie. PM1, PM10, etc.
    :param df_single_series: single-column dataframe to combine with <df_combined>
        - index is Pandas Timestamp() -- will be converted to string with minute precision
        - Column name is <measurement_fieldKey> ie. PM1, PM10, etc.
    :param sensor_name: sensor ID cooresponding to <df_single_series>. ie. S-A-001, 6261, Hawthorne, etc.
    :param measurement_fieldKey: The measurement held by <df_single_series>. ie. PM1, PM10, etc.
    :return: Update reference to <df_combined>
    """
    logger.info('********** AddSeries2Dataframe **********')

    # truncate the timestamps to minute precision
    logger.info('calling FloorTimestamp2Minute on dataframe')
    df = FloorTimestamp2Minute(df_single_series)

    # Create a new dataframe with the sensor name as the column title
    #   and the measurement as the data. Index is time (minute precision)
    logger.info('Creating new dataframe with sensor={} and Field Key={}'.format(sensor_name, measurement_fieldKey))
    df = pd.DataFrame({sensor_name: df[measurement_fieldKey]}, index=df.index)

    # First time adding a sensor this function should be passed an empty
    #   DataFrame. Just return it. Otherwise join the two into one
    if df_combined.empty:
        logger.info('new dataframe passed')
        return df
    else:

        logger.info('Joining dataframes')
        df_combined = df_combined.join(df, how='outer')

        # Sometimes there are multiple measurements with the same timestamp after
        #   truncating to the minute. Just delete the second one. To hell with it
        logger.info('Removing duplicate indices from combined dataframe')
        df_combined = df_combined[~df_combined.index.duplicated(keep='first')]

    return df_combined


def sort_alphanum(al):
    """
    Sort numerically, then alphabetically. This is done to keep sensors with numerical names (Purple Air)
    from being sorted accoring to their string value, like [1, 1100, 1200, 2, 2300] and instead sort
    numerically. Then append the alphabetically-sorted strings
    :param l: the list to sort (alphanumeric strings)
    :return: the sorted list (still alphanumeric strings)
    """
    theList = list(set(al))
    num_l = [i for i in theList if i.isdigit()]
    alp_l = sorted([i for i in theList if not i.isdigit()])
    num_l = sorted(map(int, num_l))
    return num_l + alp_l


def getSensorSource(sensorID):
    """
    Return the source given the ID name. Helpful in determining the database
    :param sensorID: sensor name. ie. S-A-001, 6264, Hawthorne, NAA, etc.
    :return: Source (string)
    """

    logger.info('********** getSensorSource({}) **********'.format(sensorID))

    DAQ_Sensors = ['Hawthorne', 'Rose Park', 'Bountiful', 'Herriman']
    MesoWest_Sensors = ['WBB', 'MTMET', 'MSI01', 'NAA']

    sensorID = str(sensorID)    # Just in case

    if 'S-A-' in sensorID:
        return 'AirU'

    elif sensorID in DAQ_Sensors:
        return 'DAQ'

    elif sensorID in MesoWest_Sensors:
        return 'MesoWest'

    else:
        try:
            int(sensorID)
            return 'Purple Air'
        except ValueError as e:
            logger.error('getSensorSource: {}'.format(str(e)))
            return False
