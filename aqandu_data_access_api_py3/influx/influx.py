import math
import time

from datetime import datetime, timedelta
from flask import jsonify, request, Blueprint, redirect, render_template, url_for, make_response
from influxdb import InfluxDBClient, DataFrameClient
from pymongo import MongoClient
# from werkzeug.local import LocalProxy
# from werkzeug.exceptions import HTTPException

import pandas as pd
import numpy as np

import logging

from flask import current_app


# from here: http://flask.pocoo.org/docs/1.0/patterns/apierrors/
class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


influx = Blueprint('influx', __name__, template_folder='templates')
# Uncaught_LOGGER = LocalProxy(lambda: current_app.logger)
LOGGER = logging.getLogger('aqandu')
uncaught_LOGGER = logging.getLogger('uncaughtExcpt')

# lookup table to transform queryString to influx column name
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
    'co': 'CO',
    'no': 'NO'
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
    'co': 'CO',
    'no': 'NO'
}


@influx.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@influx.errorhandler(500)
def exception_handler(error):
    uncaught_LOGGER.error("An uncaught exception", exc_info=True)
    # LOGGER.error("An uncaught exception", exc_info=True)

    # response = jsonify(error='An uncaught exception')
    # response.status_code = error.status_code
    # return response
    return 'An uncaught exception', 500


@influx.route('/test/online')
def online():
        LOGGER.info('********** Test/Online **********')

        # Server won't break if there's a render issue
        try:
            return render_template('online.html')
        except Exception as e:
            LOGGER.info('dashboard.html could not be rendered')
            return str(e)


@influx.route('/dbquery')
def dbquery():
    text = request.args.get('jsdata')

    try:
        dfclient = DataFrameClient(host=current_app.config['INFLUX_HOST'],
                                   port=current_app.config['INFLUX_PORT'],
                                   username=current_app.config['INFLUX_USERNAME'],
                                   password=current_app.config['INFLUX_PASSWORD'],
                                   database='airu_offline',
                                   ssl=current_app.config['SSL'],
                                   verify_ssl=current_app.config['SSL'])

        query = 'SELECT * FROM airQuality WHERE time >= now() - 2m'
        df = dfclient.query(query, chunked=True)['airQuality'].drop_duplicates(subset='ID', keep='last')

        df = df[['ID', 'PM2.5', 'Temperature', 'Humidity', 'CO', 'NO', 'SecActive']]
        df = df.sort_values(by=['ID'])

        if text:
            text = text.replace(':', '').upper()
            df = df[df['ID'].str.contains(text)]

        df['good'] = (df['PM2.5'] < 10) & \
                     (df['Temperature'] > 20) & \
                     (df['Humidity'] >= 0) & \
                     (df['CO'] > 0) & \
                     (df['NO'] > 0) & \
                     (df['SecActive'] < 3600)

        df['ID'] = [''.join(list(''.join(l + ':' * (n % 2 == 1))
                                 for n, l in enumerate(list(ID))))[:-1]
                    for ID in df['ID']]

        df_list = df.values.tolist()

    except Exception as e:
        df_list = []
        return str(e)

    return render_template('dbquery.html', table=df_list)


@influx.route("/api/dashboard")
def dashboard():

    LOGGER.info('********** Dashboard **********')

    # Server won't break if there's a render issue
    try:
        return render_template('dashboard.html')
    except Exception as e:
        LOGGER.info('dashboard.html could not be rendered')
        return str(e)


# @influx.route("/api/errorHandler/<error>")
# def error_handler(error):
#
#     LOGGER.info('********** errorHandler **********')
#     LOGGER.info('errorHandler with error={}'.format(str(error)))
#
#     try:
#         return render_template('error_template.html', anError=error)
#     except Exception as e:
#         LOGGER.info('error_template.html could not be rendered')
#         return str(e)


@influx.route("/api/get_data", methods=['POST'])
def get_data():

    LOGGER.info('********** download_file **********')

    dataType = request.form['measType']
    sensorList = request.form['sensorIDs']
    startDate = request.form['startDate']
    endDate = request.form['endDate']
    hourAvg = 'hourAvg' in request.form.keys()

    LOGGER.info(dataType)
    LOGGER.info(sensorList)
    LOGGER.info(startDate)
    LOGGER.info(endDate)

    LOGGER.info('dataType={}, sensorList={}, startDate={}, endDate={}'.format(dataType, sensorList, startDate, endDate))
    if dataType == 'Not Supported':
        msg = "Option is not supported"
        # return redirect(url_for(".error_handler", error=msg))
        raise InvalidUsage(msg, status_code=400)

    # Format the dates to the correct Influx string
    try:
        start_dt = datetime.strptime(startDate, '%Y-%m-%d')
        end_dt = datetime.strptime(endDate, '%Y-%m-%d')
    except ValueError as e:
        LOGGER.info('date conversion error: {}'.format(str(e)))
        # return redirect(url_for(".error_handler", error='ERROR: ' + str(e)))
        raise InvalidUsage('ERROR: ' + str(e), status_code=400)

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

    LOGGER.info('Sanitized sensorList: {}'.format(sensorList))

    airU_in_list = [getSensorSource(s) == 'AirU' for s in sensorList]

    if dataType != 'pm25' and not all(airU_in_list):
        LOGGER.info('unexpected data type for sensor list - redirect to errorHandler')
        # LOGGER.info(url_for("influx.dashboard"))
        # LOGGER.info(url_for(".error_handler", error='test message'))
        raise InvalidUsage('You cannot access that data type for sensors that are not AirU sensors', status_code=400)
        # return redirect(url_for(".error_handler", error='You cannot access that data type for sensors that are not AirU sensors'))

    customIDToMAC = None
    if any(airU_in_list):
        LOGGER.info('{} AirU sensors in the list. Getting sensor ID to mac dict now.'.format(airU_in_list.count(True)))

        # getting the mac address from the customID send as a query parameter
        customIDToMAC = getCustomSensorIDToMAC()

    DFclient = None
    for sensor in sensorList:

        sensorSource = getSensorSource(sensor)

        LOGGER.info('sensor={}, source={}'.format(sensor, sensorSource))

        if not sensorSource:
            LOGGER.info('Could not find the sensor source for {}, going to the next sensor'.format(sensor))
            continue

        elif sensorSource == 'AirU':

            try:
                ID = customIDToMAC[sensor]
            except ValueError as e:
                LOGGER.info('{} is an unknown ID, not in DB. Error: {}'.format(sensor, str(e)))

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

        # hour Averaging
        if hourAvg:
            dff = dff.replace(-1, np.nan)
            dff = dff.resample('H').mean()
            # avg_str = '_HR-AVG'
        # else:
            # avg_str = ''

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


@influx.route('/api/liveSensors/<sensorSource>', methods=['GET'])
def getLiveSensors(sensorSource):
    """Get sensors that are active (pushed data) since yesterday (beginning of day)"""

    LOGGER.info('*********** liveSensors request started ***********')
    LOGGER.info(sensorSource)

    if sensorSource not in ['purpleAir', 'airU', 'all']:
        LOGGER.info('sensorSource parameter is wrong')
        # abort(404)
        InvalidUsage('The sensorSource has to be one of these: purpleAir, airU, all.', status_code=404)

    # now = datetime.now()
    #
    # localTimezone = pytz.timezone('MST')
    # local_dt = localTimezone.localize(now, is_dst=None)  # now local time on server is MST, add that information to the time
    # utc_dt = local_dt.astimezone(pytz.utc)  # trasnform local now time to UTC
    utc_dt = datetime.utcnow()

    # yesterday = utc_dt - timedelta(days=1)
    nowMinus3h = utc_dt - timedelta(hours=3)
    nowMinus20m = utc_dt - timedelta(minutes=20)
    nowMinus5m = utc_dt - timedelta(minutes=5)

    # yesterdayBeginningOfDay = yesterday.replace(hour=00, minute=00, second=00)
    # yesterdayStr = yesterdayBeginningOfDay.strftime('%Y-%m-%dT%H:%M:%SZ')
    # yesterdayStr = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
    # LOGGER.info(yesterdayStr)

    nowMinus3h_str = nowMinus3h.strftime('%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info(nowMinus3h_str)

    nowMinus20m_str = nowMinus20m.strftime('%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info(nowMinus20m_str)

    nowMinus5mStr = nowMinus5m.strftime('%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info(nowMinus5mStr)

    dataSeries = []
    start = time.time()

    if sensorSource == 'purpleAir':

        # get sensors that have pushed data to the db during the last 5min
        dataSeries_purpleAir = getInfluxPollingSensors(nowMinus5mStr, "Purple Air")
        LOGGER.info('length of dataSeries_purpleAir is {}'.format(len(dataSeries_purpleAir)))

        dataSeries_mesowest = getInfluxPollingSensors(nowMinus20m_str, "Mesowest")
        LOGGER.info('length of dataSeries_mesowest is {}'.format(len(dataSeries_mesowest)))

        dataSeries_DAQ = getInfluxPollingSensors(nowMinus3h_str, "DAQ")
        LOGGER.info('length of dataSeries_DAQ is {}'.format(len(dataSeries_DAQ)))

        dataSeries = dataSeries_purpleAir + dataSeries_mesowest + dataSeries_DAQ
        LOGGER.info('length of merged dataSeries is {}'.format(len(dataSeries)))

    elif sensorSource == 'airU':

        # get sensors that have pushed data to the db during the last 5min
        dataSeries = getInfluxAirUSensors(nowMinus5mStr)
        LOGGER.info(len(dataSeries))

    elif sensorSource == 'all':

        # get sensors that have pushed data to the db during the last 5min
        LOGGER.info('get all dataSeries started')

        dataSeries_purpleAir = getInfluxPollingSensors(nowMinus3h_str, "Purple Air")
        LOGGER.info('length of dataSeries_purpleAir is {}'.format(len(dataSeries_purpleAir)))

        dataSeries_mesowest = getInfluxPollingSensors(nowMinus20m_str, "Mesowest")
        LOGGER.info('length of dataSeries_mesowest is {}'.format(len(dataSeries_mesowest)))

        dataSeries_DAQ = getInfluxPollingSensors(nowMinus3h_str, "DAQ")
        LOGGER.info('length of dataSeries_DAQ is {}'.format(len(dataSeries_DAQ)))

        airUDataSeries = getInfluxAirUSensors(nowMinus5mStr)
        LOGGER.info(len(airUDataSeries))
        LOGGER.debug(airUDataSeries)

        dataSeries = dataSeries_purpleAir + dataSeries_mesowest + dataSeries_DAQ + airUDataSeries
        LOGGER.info(len(dataSeries))
        LOGGER.debug(dataSeries)

        # from https://stackoverflow.com/questions/38279269/python-comparing-each-item-of-a-list-to-every-other-item-in-that-list by Kevin
        lats = dict()
        for idx, sensor in enumerate(dataSeries):
            if sensor['Latitude'] in lats:
                lats[sensor['Latitude']].append(idx)
            else:
                lats[sensor['Latitude']] = [idx]

        for lat, sameLats in lats.items():
            if len(sameLats) > 1:
                for i in range(len(sameLats)):
                    for j in range(i + 1, len(sameLats)):
                        if dataSeries[sameLats[i]]['ID'] != dataSeries[sameLats[j]]['ID'] and dataSeries[sameLats[i]]['Longitude'] == dataSeries[sameLats[j]]['Longitude']:
                            dataSeries[sameLats[j]]['Longitude'] = str(float(dataSeries[sameLats[j]]['Longitude']) - 0.0005)

# for i in range(len(dataSeries)):
#     # LOGGER.info('i is %s', i)
#     for j in range(i + 1, len(dataSeries)):
#         # LOGGER.info('j is %s', j)
#         # LOGGER.info('dataSeries[i] is %s', dataSeries[i])
#         # LOGGER.info('dataSeries[j] is %s', dataSeries[j])
#         if dataSeries[i]['ID'] != dataSeries[j]['ID']:
#             if dataSeries[i]['Latitude'] == dataSeries[j]['Latitude'] and dataSeries[i]['Longitude'] == dataSeries[j]['Longitude']:
#                 dataSeries[j]['Longitude'] = str(float(dataSeries[j]['Longitude']) - 0.0005)

        LOGGER.info('get all dataSeries done')
    else:
        LOGGER.info('wrong path is not catched')
        # abort(404)
        InvalidUsage('Something is really wrong, here!!', status_code=404)

    end = time.time()

    print("*********** Time to download:", end - start)

    return jsonify(dataSeries)


# /api/rawDataFrom?id=1010&sensorSource=PurpleAir&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=all
# /api/rawDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&show=pm25,pm1
@influx.route('/api/rawDataFrom', methods=['GET'])
def getRawDataFrom():

    airUdbs = ['altitude', 'humidity', 'latitude', 'longitude', 'pm1', 'pm25', 'pm10', 'posix', 'secActive', 'temperature', 'co', 'no']

    LOGGER.info('*********** rawDataFrom request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    dataSeries = []
    if queryParameters['sensorSource'] == 'airu':
        LOGGER.info('airu')
        LOGGER.info(queryParameters['sensorSource'])

        start = time.time()

        # create createSelection
        whatToShow = queryParameters['show'].split(',')
        LOGGER.info(whatToShow)

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
            LOGGER.info('this is an unknow ID, not in db')
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
            LOGGER.info(aDB)

            if aDB == 'pm25':
                # normalizing to pm25
                fieldString = lookupParameterToAirUInflux.get(aDB) + ' AS pm25'
            else:
                fieldString = lookupParameterToAirUInflux.get(aDB)

            queryAirU = "SELECT " + fieldString + " FROM " + aDB + " " \
                        "WHERE ID = '" + theID + "' " \
                        "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

            LOGGER.info(queryAirU)

            dataAirU = influxClientAirU.query(queryAirU, epoch=None, chunked=True)
            dataAirU = dataAirU.raw

            # LOGGER.info(dataAirU)

            # check if query gave data back
            if 'series' in dataAirU:

                concatenatedSeries = []
                for aSerie in dataAirU['series']:

                    valuesAirU = aSerie['values']
                    columnsAirU = aSerie['columns']

                    # LOGGER.info(len(valuesAirU))
                    # LOGGER.info(len(columnsAirU))

                    concatenatedSeries = concatenatedSeries + list(map(lambda x: dict(zip(columnsAirU, x)), valuesAirU))

                if not dataSeries:
                    dataSeries = concatenatedSeries
                    # LOGGER.info(len(dataSeries))
                else:
                    newDataSeries = concatenatedSeries
                    # LOGGER.info(len(newDataSeries))

                    # print(list(zip(dataSeries, newDataSeries)))
                    # as a security I add the timestamp from the merged db, the difference in timestamps are in the 0.1 milisecond (0.0001)
                    # dataSeries = list(map(lambda y: {**y[0], **y[1], 'time_' + aDB: y[1]['time']} if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0] else {0}, list(zip(dataSeries, newDataSeries))))

                    # LOGGER.info(len(dataSeries))
                    # LOGGER.info(len(newDataSeries))

                    tmpList = []
                    for dict1, dict2 in list(zip(dataSeries, newDataSeries)):
                        # print(elem1, elem2)
                        # LOGGER.info(dict1)
                        # LOGGER.info(dict2)

                        if dict1['time'].split('.')[0][:-3] == dict2['time'].split('.')[0][:-3]:
                            LOGGER.debug('equal')
                            # replace the time attribute with a new key so it does not copy over the dict1's time when being merged
                            dict2['time_' + aDB] = dict2.pop('time')
                            mergedObject = mergeTwoDicts(dict1, dict2)

                            tmpList.append(mergedObject)

                    LOGGER.info(len(tmpList))
                    dataSeries = tmpList

                # dataSeries = [{y[0], y[1]} for elem in list(zip(dataSeries, newDataSeries)) if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0]]

        queryForTags = "SELECT LAST(" + lookupParameterToAirUInflux.get("pm25") + "), ID, \"SensorModel\" FROM pm25 " \
                       "WHERE ID = '" + theID + "' "
        LOGGER.info(queryForTags)

        dataTags = influxClientAirU.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        # LOGGER.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        dataSeries_Tags[0]['Sensor Source'] = 'airu'
        # LOGGER.info(dataSeries_Tags)

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
        LOGGER.info(selectString)

        query = "SELECT " + selectString + " FROM airQuality " \
                "WHERE ID = '" + queryParameters['id'] + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "
        LOGGER.info(query)

        start = time.time()

        data = influxClientPolling.query(query, epoch=None)

        # vies me an internal server error ---> maybe there is no data for this sensor at that time?? check!! if that is the case make it so the program does not crash --> output reasonable error message.
        #
        # https://air.eng.utah.edu/dbapi/api/rawDataFrom?id=6271&sensorSource=Purple%20Air&start=2018-07-24T07:00:00Z&end=2018-07-25T07:00:00Z&show=pm25
        #
        #
        # There is no data for 6315 for this timeframe:
        #  [getRawDataFrom:587]
        #  Output reasonable error message if the query is empty, not braking everything.
        #
        #
        # SELECT "pm2.5 (ug/m^3)" AS pm25 FROM airQuality WHERE ID = '6315' AND time >= '2018-07-24T07:00:00Z' AND time <= '2018-07-25T07:00:00Z'

        if len(data) > 0:
            # query gave back data
            data = data.raw

            LOGGER.info(data['series'][0]['values'][0])

            theValues = data['series'][0]['values']
            theColumns = data['series'][0]['columns']

            # pmTimeSeries = list(map(lambda x: {'time': x[0], 'pm25': x[1]}, theValues))
            dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))

            queryForTags = "SELECT LAST(" + lookupQueryParameterToInflux.get("pm25") + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality " \
                           "WHERE ID = '" + queryParameters['id'] + "' "
            LOGGER.info(queryForTags)

            dataTags = influxClientPolling.query(queryForTags, epoch=None)
            dataTags = dataTags.raw
            LOGGER.info(dataTags)

            dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
            LOGGER.info(dataSeries_Tags)

            newDataSeries = {}
            newDataSeries["data"] = dataSeries
            newDataSeries["tags"] = dataSeries_Tags

        else:
            # query gave back nothing, means there is no data for sensor with ID queryParameters['id'] for the time range = [queryParameters['start'], queryParameters['end']

            newDataSeries = {}
            newDataSeries["data"] = []
            newDataSeries["tags"] = []

        end = time.time()

    LOGGER.info('*********** Time to download: %s ***********', end - start)

    # LOGGER.info(newDataSeries["data"])
    # LOGGER.info(len(newDataSeries["data"]))

    resp = jsonify(newDataSeries)
    resp.status_code = 200

    return resp


@influx.route('/api/debug_rawData', methods=['GET'])
def getDebugRawData():

    airUdbs = ['altitude', 'humidity', 'latitude', 'longitude', 'pm1', 'pm25', 'pm10', 'posix', 'secActive', 'temperature', 'co', 'no']

    LOGGER.info('*********** DEBUGGING rawDataFrom request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    dataSeries = []
    if queryParameters['sensorSource'] == 'airu':
        LOGGER.info('airu')
        LOGGER.info(queryParameters['sensorSource'])

        start = time.time()

        # create createSelection
        whatToShow = queryParameters['show'].split(',')
        LOGGER.info(whatToShow)

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
            LOGGER.info('this is an unknow ID, not in db')
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
            LOGGER.info(aDB)

            if aDB == 'pm25':
                # normalizing to pm25
                fieldString = lookupParameterToAirUInflux.get(aDB) + ' AS pm25'
            else:
                fieldString = lookupParameterToAirUInflux.get(aDB)

            queryAirU = "SELECT " + fieldString + " FROM " + aDB + " " \
                        "WHERE ID = '" + theID + "' " \
                        "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

            LOGGER.info(queryAirU)

            dataAirU = influxClientAirU.query(queryAirU, epoch=None, chunked=True)
            dataAirU = dataAirU.raw

            LOGGER.info(dataAirU)

            # check if query gave data back
            if 'series' in dataAirU:

                concatenatedSeries = []
                for aSerie in dataAirU['series']:

                    valuesAirU = aSerie['values']
                    columnsAirU = aSerie['columns']

                    # LOGGER.info(len(valuesAirU))
                    # LOGGER.info(len(columnsAirU))

                    concatenatedSeries = concatenatedSeries + list(map(lambda x: dict(zip(columnsAirU, x)), valuesAirU))

                if not dataSeries:
                    dataSeries = concatenatedSeries
                    # LOGGER.info(len(dataSeries))
                else:
                    newDataSeries = concatenatedSeries
                    # LOGGER.info(len(newDataSeries))

                    # print(list(zip(dataSeries, newDataSeries)))
                    # as a security I add the timestamp from the merged db, the difference in timestamps are in the 0.1 milisecond (0.0001)
                    # dataSeries = list(map(lambda y: {**y[0], **y[1], 'time_' + aDB: y[1]['time']} if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0] else {0}, list(zip(dataSeries, newDataSeries))))

                    # LOGGER.info(len(dataSeries))
                    # LOGGER.info(len(newDataSeries))

                    tmpList = []
                    for dict1, dict2 in list(zip(dataSeries, newDataSeries)):
                        # print(elem1, elem2)
                        # LOGGER.info(dict1)
                        # LOGGER.info(dict2)

                        if dict1['time'].split('.')[0][:-3] == dict2['time'].split('.')[0][:-3]:
                            LOGGER.info('equal')
                            # replace the time attribute with a new key so it does not copy over the dict1's time when being merged
                            dict2['time_' + aDB] = dict2.pop('time')
                            mergedObject = mergeTwoDicts(dict1, dict2)

                            tmpList.append(mergedObject)

                    LOGGER.info(len(tmpList))
                    dataSeries = tmpList

                # dataSeries = [{y[0], y[1]} for elem in list(zip(dataSeries, newDataSeries)) if y[0]['time'].split('.')[0] == y[1]['time'].split('.')[0]]

        queryForTags = "SELECT LAST(" + lookupParameterToAirUInflux.get("pm25") + "), ID, \"SensorModel\" FROM pm25 " \
                       "WHERE ID = '" + theID + "' "
        LOGGER.info(queryForTags)

        dataTags = influxClientAirU.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        # LOGGER.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        dataSeries_Tags[0]['Sensor Source'] = 'airu'
        # LOGGER.info(dataSeries_Tags)

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
        LOGGER.info(selectString)

        query = "SELECT " + selectString + " FROM airQuality " \
                "WHERE ID = '" + queryParameters['id'] + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' "

        LOGGER.info(query)

        start = time.time()

        data = influxClientPolling.query(query, epoch=None)

        # vies me an internal server error ---> maybe there is no data for this sensor at that time?? check!! if that is the case make it so the program does not crash --> output reasonable error message.
        #
        # https://air.eng.utah.edu/dbapi/api/rawDataFrom?id=6271&sensorSource=Purple%20Air&start=2018-07-24T07:00:00Z&end=2018-07-25T07:00:00Z&show=pm25
        #
        #
        # There is no data for 6315 for this timeframe:
        #  [getRawDataFrom:587]
        #  Output reasonable error message if the query is empty, not braking everything.
        #
        #
        # SELECT "pm2.5 (ug/m^3)" AS pm25 FROM airQuality WHERE ID = '6315' AND time >= '2018-07-24T07:00:00Z' AND time <= '2018-07-25T07:00:00Z'

        if len(data) > 0:
            # query gave back data
            data = data.raw

            LOGGER.info(data['series'][0]['values'][0])

            theValues = data['series'][0]['values']
            theColumns = data['series'][0]['columns']

            # pmTimeSeries = list(map(lambda x: {'time': x[0], 'pm25': x[1]}, theValues))
            dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))

            queryForTags = "SELECT LAST(" + lookupQueryParameterToInflux.get("pm25") + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality " \
                           "WHERE ID = '" + queryParameters['id'] + "' "
            LOGGER.info(queryForTags)

            dataTags = influxClientPolling.query(queryForTags, epoch=None)
            dataTags = dataTags.raw
            LOGGER.info(dataTags)

            dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
            LOGGER.info(dataSeries_Tags)

            newDataSeries = {}
            newDataSeries["data"] = dataSeries
            newDataSeries["tags"] = dataSeries_Tags

        else:
            # query gave back nothing, means there is no data for sensor with ID queryParameters['id'] for the time range = [queryParameters['start'], queryParameters['end']

            newDataSeries = {}
            newDataSeries["data"] = []
            newDataSeries["tags"] = []

        end = time.time()

    LOGGER.info('*********** Time to download: %s ***********', end - start)

    # LOGGER.info(newDataSeries["data"])
    # LOGGER.info(len(newDataSeries["data"]))

    resp = jsonify(newDataSeries)
    resp.status_code = 200

    return resp


# http://0.0.0.0:5000/api/processedDataFrom?id=1010&start=2017-10-01T00:00:00Z&end=2017-10-02T00:00:00Z&function=mean&functionArg=pm25&timeInterval=30m
@influx.route('/api/processedDataFrom', methods=['GET'])
def getProcessedDataFrom():

    LOGGER.info('*********** processedDataFrom request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    minutesOffset = queryParameters['start'].split(':')[1] + 'm'
    LOGGER.info(minutesOffset)

    if queryParameters['sensorSource'] == 'airu':
        LOGGER.info('airu')
        LOGGER.info(queryParameters['sensorSource'])

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
            LOGGER.info('this is an unknow ID, not in db')
            message = {'status': 404,
                       'message': 'unknown ID, so sensor with that ID'
                       }
            errorResp = jsonify(message)
            errorResp.headers.add('Access-Control-Allow-Origin', '*')
            errorResp.status_code = 404

            return errorResp

        LOGGER.info('does it go on')
        selectString = createSelection('processed', queryParameters)
        LOGGER.info(selectString)

        query = "SELECT " + selectString + " " \
                "WHERE ID = '" + theID + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' GROUP BY time(" + queryParameters['timeInterval'] + ", " + minutesOffset + ")"
        LOGGER.info(query)

        start = time.time()

        data = influxClientAirU.query(query, epoch=None)
        data = data.raw
        # LOGGER.info(data)
# TODO need to check these dictionary call for validity first!!!!!!! else output an error message
        # parse the data

        # if 'series' in data:
        theValues = data['series'][0]['values']
        theColumns = data['series'][0]['columns']

        dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))
        # pmTimeSeries = list(map(lambda x: {time: x[0], 'pm2.5 (ug/m^3)': x[1]}, theValues))

        # print(pmTimeSeries)

        queryForTags = "SELECT LAST(" + lookupParameterToAirUInflux.get(queryParameters['functionArg']) + "), ID, \"SensorModel\" " \
                       "FROM " + queryParameters['functionArg'] + " " \
                       "WHERE ID = '" + theID + "' "
        LOGGER.info(queryForTags)

        dataTags = influxClientAirU.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        # LOGGER.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        dataSeries_Tags[0]['Sensor Source'] = 'airu'
        # LOGGER.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags
        # else:
        #     msg = 'database returned no data, either there is no data with these criteria or you have an error in your query'
        #     raise InvalidUsage(msg, status_code=400)

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
        # LOGGER.info(queryParameters)

        # TODO do some parameter checking
        # TODO check if queryParameters exist if not write excpetion

        selectString = createSelection('processed', queryParameters)
        LOGGER.info(selectString)

        query = "SELECT " + selectString + " " \
                "WHERE ID = '" + queryParameters['id'] + "' " \
                "AND time >= '" + queryParameters['start'] + "' AND time <= '" + queryParameters['end'] + "' GROUP BY time(" + queryParameters['timeInterval'] + ", " + minutesOffset + ")"
        LOGGER.info(query)

        start = time.time()

        data = influxClientPolling.query(query, epoch=None)
        data = data.raw
        # LOGGER.info(data)

        # parse the data
        theValues = data['series'][0]['values']
        theColumns = data['series'][0]['columns']

        dataSeries = list(map(lambda x: dict(zip(theColumns, x)), theValues))
        # pmTimeSeries = list(map(lambda x: {time: x[0], 'pm2.5 (ug/m^3)': x[1]}, theValues))

        # print(pmTimeSeries)

        queryForTags = "SELECT LAST(" + lookupQueryParameterToInflux.get(queryParameters['functionArg']) + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality WHERE ID = '" + queryParameters['id'] + "' "

        dataTags = influxClientPolling.query(queryForTags, epoch=None)
        dataTags = dataTags.raw
        # LOGGER.info(dataTags)

        dataSeries_Tags = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), dataTags['series']))
        # LOGGER.info(dataSeries_Tags)

        newDataSeries = {}
        newDataSeries["data"] = dataSeries
        newDataSeries["tags"] = dataSeries_Tags

        end = time.time()

    LOGGER.info('*********** Time to download: %s ***********', end - start)

    resp = jsonify(newDataSeries)
    resp.status_code = 200

    return resp


# http://0.0.0.0:5000/api/lastValue?fieldKey=pm25
@influx.route('/api/lastValue', methods=['GET'])
def getLastValuesForLiveSensor():

    LOGGER.info('*********** lastPM request started ***********')

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_POLLING_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    queryParameters = request.args
    LOGGER.info(queryParameters['fieldKey'])

    queryPolling = "SELECT LAST(" + lookupQueryParameterToInflux.get(queryParameters['fieldKey']) + "), ID, \"Sensor Model\", \"Sensor Source\" FROM airQuality GROUP BY ID"
    LOGGER.info(queryPolling)

    data = influxClientPolling.query(queryPolling, epoch=None)
    data = data.raw
    LOGGER.debug(data)

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

    queryAirU = "SELECT LAST(" + lookupParameterToAirUInflux.get(queryParameters['fieldKey']) + "), ID, \"SensorModel\" FROM " + queryParameters['fieldKey'] + " GROUP BY ID"
    LOGGER.info(queryAirU)

    dataAirU = influxClientAirU.query(queryAirU, epoch=None)
    dataAirU = dataAirU.raw
    LOGGER.debug(dataAirU)

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

    LOGGER.info('*********** lastPM request done ***********')

    return jsonify(allLastValues)


# contour API calls
@influx.route('/api/contours', methods=['GET'])
def getContours():

    LOGGER.info('*********** getting contours request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    startDate = queryParameters['start']
    LOGGER.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    LOGGER.info('the start date')
    LOGGER.info(startDate)
    LOGGER.info('the end date')
    LOGGER.info(endDate)

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
    allHighEstimates = db.timeSlicedEstimates_high.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)
    # lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)

    contours = []

    LOGGER.info('the allHighEstimates')
    LOGGER.info(allHighEstimates.count())
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        LOGGER.info(estimationDateSliceDateHigh)
        contours.append({'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})

    LOGGER.info('the lowEstimates')
    # LOGGER.info(lowEstimates.count())

    # lowEstimates.batch_size(10000)

    LOGGER.info('date range')
    for aDate in pd.date_range(startDate, endDate, freq='12h')[1:]:
        LOGGER.info(aDate)
        lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": aDate}}).sort('estimationFor', -1)

        for estimateSliceLow in lowEstimates:
            estimationDateSliceDateLow = estimateSliceLow['estimationFor']
            LOGGER.info(estimationDateSliceDateLow)
            contours.append({'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

        startDate = aDate

    # for estimateSliceLow in lowEstimates:
    #     estimationDateSliceDateLow = estimateSliceLow['estimationFor']
    #     LOGGER.info(estimationDateSliceDateLow)
    #     contours.append({'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

    # LOGGER.info(contours)
    #
    # LOGGER.info(jsonify(contours))

    resp = jsonify(contours)
    resp.status_code = 200

    LOGGER.info('*********** getting contours request done ***********')

    return resp


@influx.route('/api/contours_debugging', methods=['GET'])
def getContours_debugging():

    LOGGER.info('*********** getting contours request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    startDate = queryParameters['start']
    LOGGER.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    LOGGER.info('the start date')
    LOGGER.info(startDate)
    LOGGER.info('the end date')
    LOGGER.info(endDate)

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
    allHighEstimates = db.timeSlicedEstimates_debug_high.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)
    # lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)

    contours = []

    LOGGER.info('the allHighEstimates')
    LOGGER.info(allHighEstimates.count())
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        LOGGER.info(estimationDateSliceDateHigh)
        contours.append({'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})

    LOGGER.info('the lowEstimates')
    # LOGGER.info(lowEstimates.count())

    # lowEstimates.batch_size(10000)

    LOGGER.info('date range')
    for aDate in pd.date_range(startDate, endDate, freq='12h')[1:]:
        LOGGER.info(aDate)
        lowEstimates = db.timeSlicedEstimates_debug_low.find({"estimationFor": {"$gte": startDate, "$lt": aDate}}).sort('estimationFor', -1)

        for estimateSliceLow in lowEstimates:
            estimationDateSliceDateLow = estimateSliceLow['estimationFor']
            LOGGER.info(estimationDateSliceDateLow)
            contours.append({'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

        startDate = aDate

    # for estimateSliceLow in lowEstimates:
    #     estimationDateSliceDateLow = estimateSliceLow['estimationFor']
    #     LOGGER.info(estimationDateSliceDateLow)
    #     contours.append({'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})

    # LOGGER.info(contours)
    #
    # LOGGER.info(jsonify(contours))

    resp = jsonify(contours)
    resp.status_code = 200

    LOGGER.info('*********** getting contours request done ***********')

    return resp


@influx.route('/api/getLatestContour', methods=['GET'])
def getLatestContour():

    LOGGER.info('*********** getting latest contours request started ***********')

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
    LOGGER.info(cursor)

    for doc in cursor:
        LOGGER.info(type(doc))
        LOGGER.info(doc['estimate'])
        LOGGER.info(doc['contours'])
        # LOGGER.info(jsonify(doc))

        lastContour = {'contour': doc['contours'], 'date_utc': doc['estimationFor']}

    # LOGGER.info(contours)

    # LOGGER.info(jsonify(contours))

    resp = jsonify(lastContour)
    resp.status_code = 200

    LOGGER.info('*********** getting latest contours request done ***********')

    return resp


@influx.route('/api/getLatestContour_debugging', methods=['GET'])
def getLatestContour_debugging():

    LOGGER.info('*********** getting latest contours request started ***********')

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    # contours = {}

    # TODO collection_name in db.collection_names() to check if collection exists
    cursor = db.timeSlicedEstimates_debug_high.find().sort('estimationFor', -1).limit(1)
    LOGGER.info(cursor)

    for doc in cursor:
        LOGGER.info(type(doc))
        LOGGER.info(doc['estimate'])
        LOGGER.info(doc['contours'])
        # LOGGER.info(jsonify(doc))

        lastContour = {'contour': doc['contours'], 'date_utc': doc['estimationFor']}

    # LOGGER.info(contours)

    # LOGGER.info(jsonify(contours))

    resp = jsonify(lastContour)
    resp.status_code = 200

    LOGGER.info('*********** getting latest contours request done ***********')

    return resp


@influx.route('/api/getEstimatesForLocation', methods=['GET'])
def getEstimatesForLocation():
    # need a location and the needed timespan

    LOGGER.info('*********** getEstimatesForLocation started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    location_lat = queryParameters['location_lat']
    location_lng = queryParameters['location_lng']
    startDate = queryParameters['start']
    # LOGGER.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    # LOGGER.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    LOGGER.info('the start date')
    LOGGER.info(startDate)
    LOGGER.info('the end date')
    LOGGER.info(endDate)

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

    LOGGER.info(gridInfo)

    theCorners = {}
    if gridInfo is not None:
        theGrid = gridInfo['transformedGrid']
        numberGridCells_LAT = gridInfo['numberOfGridCells']['lat']
        numberGridCells_LONG = gridInfo['numberOfGridCells']['long']

        LOGGER.info(theGrid)
        LOGGER.info(numberGridCells_LAT)
        LOGGER.info(numberGridCells_LONG)

        topRightCornerIndex = str((int(numberGridCells_LAT + 1) * int(numberGridCells_LONG + 1)) - 1)
        bottomLeftCornerIndex = str(0)

        stepSizeLat = abs(theGrid[topRightCornerIndex]['lat'][0] - theGrid[bottomLeftCornerIndex]['lat'][0]) / numberGridCells_LAT
        stepSizeLong = abs(theGrid[topRightCornerIndex]['lngs'][0] - theGrid[bottomLeftCornerIndex]['lngs'][0]) / numberGridCells_LONG

        LOGGER.info(stepSizeLat)
        LOGGER.info(stepSizeLong)

        fourCorners_left_index_lng = math.floor((float(location_lng) - theGrid[bottomLeftCornerIndex]['lngs'][0]) / stepSizeLong)
        fourCorners_bottom_index_lat = math.floor((float(location_lat) - theGrid[bottomLeftCornerIndex]['lat'][0]) / stepSizeLat)

        LOGGER.info(fourCorners_left_index_lng)
        LOGGER.info(fourCorners_bottom_index_lat)

        # leftCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_left_index_x * stepSizeLong)
        # rightCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_right_index_x * stepSizeLong)
        # bottomCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_bottom_index_y * stepSizeLat)
        # topCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_top_index_y * stepSizeLat)

        leftBottomCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        leftTopCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))
        rightBottomCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        rightTopCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))

        LOGGER.info(leftBottomCorner_index)
        LOGGER.info(rightBottomCorner_index)
        LOGGER.info(leftTopCorner_index)
        LOGGER.info(rightTopCorner_index)

        leftBottomCorner_location = theGrid[leftBottomCorner_index]
        rightBottomCorner_location = theGrid[rightBottomCorner_index]
        leftTopCorner_location = theGrid[leftTopCorner_index]
        rightTopCorner_location = theGrid[rightTopCorner_index]

        LOGGER.info(leftBottomCorner_location)
        LOGGER.info(rightBottomCorner_location)
        LOGGER.info(leftTopCorner_location)
        LOGGER.info(rightTopCorner_location)

        theCorners['leftBottomCorner'] = {'lat': leftBottomCorner_location['lat'][0], 'lng': leftBottomCorner_location['lngs'][0]}
        theCorners['rightBottomCorner'] = {'lat': rightBottomCorner_location['lat'][0], 'lng': rightBottomCorner_location['lngs'][0]}
        theCorners['leftTopCorner'] = {'lat': leftTopCorner_location['lat'][0], 'lng': leftTopCorner_location['lngs'][0]}
        theCorners['rightTopCorner'] = {'lat': rightTopCorner_location['lat'][0], 'lng': rightTopCorner_location['lngs'][0]}

    else:
        LOGGER.info('grid info is none!')

    # get the 4 corners for each timestamp between the timespan
    # take all estimates in timeSpan

    # first take estimates from high collection
    # then estimates from low collection
    allHighEstimates = db.timeSlicedEstimates_high.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)
    lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)

    x = float(location_lng)
    y = float(location_lat)
    x1 = leftBottomCorner_location['lngs'][0]
    x2 = rightBottomCorner_location['lngs'][0]
    y1 = leftBottomCorner_location['lat'][0]
    y2 = leftTopCorner_location['lat'][0]

    # theDates = []
    theInterpolatedValues = []
    LOGGER.info('the allHighEstimates')
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        # theDates.append({'date': estimationDateSliceDateHigh, 'origin': 'high'})
        # LOGGER.info(estimationDateSliceDateHigh)

        # get the corner values
        leftBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftBottomCorner_index]['pm25']
        LOGGER.debug(leftBottomCorner_pm25valueHigh)
        rightBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightBottomCorner_index]['pm25']
        LOGGER.debug(rightBottomCorner_pm25valueHigh)
        leftTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftTopCorner_index]['pm25']
        LOGGER.debug(leftTopCorner_pm25valueHigh)
        rightTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightTopCorner_index]['pm25']
        LOGGER.debug(rightTopCorner_pm25valueHigh)

        Q11 = leftBottomCorner_pm25valueHigh
        Q21 = rightBottomCorner_pm25valueHigh
        Q12 = leftTopCorner_pm25valueHigh
        Q22 = rightTopCorner_pm25valueHigh

        # do bilinear interpolation using these 4 corners
        interpolatedEstimateHigh = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)
        LOGGER.info(interpolatedEstimateHigh)

        # theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateHigh, 'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})
        theInterpolatedValues.append({'pm25': interpolatedEstimateHigh, 'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'origin': 'high'})

    LOGGER.info('the lowEstimates')
    LOGGER.info(lowEstimates.count())
    for estimateSliceLow in lowEstimates:
        # LOGGER.info(estimateSliceLow)
        estimationDateSliceDateLow = estimateSliceLow['estimationFor']
        # theDates.append({'date': estimationDateSliceDateLow, 'origin': 'low'})
        LOGGER.info(estimationDateSliceDateLow)

        leftBottomCorner_pm25valueLow = estimateSliceLow['estimate'][leftBottomCorner_index]['pm25']
        rightBottomCorner_pm25valueLow = estimateSliceLow['estimate'][rightBottomCorner_index]['pm25']
        leftTopCorner_pm25valueLow = estimateSliceLow['estimate'][leftTopCorner_index]['pm25']
        rightTopCorner_pm25valueLow = estimateSliceLow['estimate'][rightTopCorner_index]['pm25']

        Q11 = leftBottomCorner_pm25valueLow
        Q21 = rightBottomCorner_pm25valueLow
        Q12 = leftTopCorner_pm25valueLow
        Q22 = rightTopCorner_pm25valueLow

        interpolatedEstimateLow = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)

        # theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateLow, 'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})
        theInterpolatedValues.append({'pm25': interpolatedEstimateLow, 'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'origin': 'low'})

        LOGGER.info('done with lowEstimates')

    LOGGER.info(theCorners)

    resp = jsonify(theInterpolatedValues)
    resp.status_code = 200

    LOGGER.info('*********** getting getEstimatesForLocation request done ***********')

    return resp


@influx.route('/api/getEstimatesForLocation_debugging', methods=['GET'])
def getEstimatesForLocation_debugging():
    # need a location and the needed timespan

    LOGGER.info('*********** DEBUGGING getEstimatesForLocation started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    location_lat = queryParameters['location_lat']
    location_lng = queryParameters['location_lng']
    startDate = queryParameters['start']
    # LOGGER.info(startDate)
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    # LOGGER.info(startDate)
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    LOGGER.info('the start date')
    LOGGER.info(startDate)
    LOGGER.info('the end date')
    LOGGER.info(endDate)

    # use location to get the 4 estimation data corners
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    gridInfo = db.estimationMetadata.find_one({"metadataType": 'timeSlicedEstimates_debug_low', "gridID": 4})

    LOGGER.info(gridInfo)

    theCorners = {}
    if gridInfo is not None:
        theGrid = gridInfo['transformedGrid']
        numberGridCells_LAT = gridInfo['numberOfGridCells']['lat']
        numberGridCells_LONG = gridInfo['numberOfGridCells']['long']

        LOGGER.info(theGrid)
        LOGGER.info(numberGridCells_LAT)
        LOGGER.info(numberGridCells_LONG)

        topRightCornerIndex = str((int(numberGridCells_LAT + 1) * int(numberGridCells_LONG + 1)) - 1)
        bottomLeftCornerIndex = str(0)

        stepSizeLat = abs(theGrid[topRightCornerIndex]['lat'][0] - theGrid[bottomLeftCornerIndex]['lat'][0]) / numberGridCells_LAT
        stepSizeLong = abs(theGrid[topRightCornerIndex]['lngs'][0] - theGrid[bottomLeftCornerIndex]['lngs'][0]) / numberGridCells_LONG

        LOGGER.info(stepSizeLat)
        LOGGER.info(stepSizeLong)

        fourCorners_left_index_lng = math.floor((float(location_lng) - theGrid[bottomLeftCornerIndex]['lngs'][0]) / stepSizeLong)
        fourCorners_bottom_index_lat = math.floor((float(location_lat) - theGrid[bottomLeftCornerIndex]['lat'][0]) / stepSizeLat)

        LOGGER.info(fourCorners_left_index_lng)
        LOGGER.info(fourCorners_bottom_index_lat)

        # leftCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_left_index_x * stepSizeLong)
        # rightCorner_long = theGrid[bottomLeftCornerIndex]['long'] + (fourCorners_right_index_x * stepSizeLong)
        # bottomCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_bottom_index_y * stepSizeLat)
        # topCorner_lat = theGrid[bottomLeftCornerIndex]['lat'] + (fourCorners_top_index_y * stepSizeLat)

        leftBottomCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        leftTopCorner_index = str((fourCorners_left_index_lng * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))
        rightBottomCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + fourCorners_bottom_index_lat)
        rightTopCorner_index = str(((fourCorners_left_index_lng + 1) * (numberGridCells_LAT + 1)) + (fourCorners_bottom_index_lat + 1))

        LOGGER.info(leftBottomCorner_index)
        LOGGER.info(rightBottomCorner_index)
        LOGGER.info(leftTopCorner_index)
        LOGGER.info(rightTopCorner_index)

        leftBottomCorner_location = theGrid[leftBottomCorner_index]
        rightBottomCorner_location = theGrid[rightBottomCorner_index]
        leftTopCorner_location = theGrid[leftTopCorner_index]
        rightTopCorner_location = theGrid[rightTopCorner_index]

        LOGGER.info(leftBottomCorner_location)
        LOGGER.info(rightBottomCorner_location)
        LOGGER.info(leftTopCorner_location)
        LOGGER.info(rightTopCorner_location)

        theCorners['leftBottomCorner'] = {'lat': leftBottomCorner_location['lat'][0], 'lng': leftBottomCorner_location['lngs'][0]}
        theCorners['rightBottomCorner'] = {'lat': rightBottomCorner_location['lat'][0], 'lng': rightBottomCorner_location['lngs'][0]}
        theCorners['leftTopCorner'] = {'lat': leftTopCorner_location['lat'][0], 'lng': leftTopCorner_location['lngs'][0]}
        theCorners['rightTopCorner'] = {'lat': rightTopCorner_location['lat'][0], 'lng': rightTopCorner_location['lngs'][0]}

    else:
        LOGGER.info('grid info is none!')

    # get the 4 corners for each timestamp between the timespan
    # take all estimates in timeSpan

    # first take estimates from high collection
    # then estimates from low collection
    allHighEstimates = db.timeSlicedEstimates_debug_high.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)
    lowEstimates = db.timeSlicedEstimates_debug_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', -1)

    x = float(location_lng)
    y = float(location_lat)
    x1 = leftBottomCorner_location['lngs'][0]
    x2 = rightBottomCorner_location['lngs'][0]
    y1 = leftBottomCorner_location['lat'][0]
    y2 = leftTopCorner_location['lat'][0]

    # theDates = []
    theInterpolatedValues = []
    LOGGER.info('the allHighEstimates')
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']
        # theDates.append({'date': estimationDateSliceDateHigh, 'origin': 'high'})
        # LOGGER.info(estimationDateSliceDateHigh)

        # get the corner values
        leftBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftBottomCorner_index]['pm25']
        LOGGER.debug(leftBottomCorner_pm25valueHigh)
        rightBottomCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightBottomCorner_index]['pm25']
        LOGGER.debug(rightBottomCorner_pm25valueHigh)
        leftTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][leftTopCorner_index]['pm25']
        LOGGER.debug(leftTopCorner_pm25valueHigh)
        rightTopCorner_pm25valueHigh = estimateSliceHigh['estimate'][rightTopCorner_index]['pm25']
        LOGGER.debug(rightTopCorner_pm25valueHigh)

        Q11 = leftBottomCorner_pm25valueHigh
        Q21 = rightBottomCorner_pm25valueHigh
        Q12 = leftTopCorner_pm25valueHigh
        Q22 = rightTopCorner_pm25valueHigh

        # do bilinear interpolation using these 4 corners
        interpolatedEstimateHigh = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)
        LOGGER.info(interpolatedEstimateHigh)

        # theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateHigh, 'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceHigh['contours'], 'origin': 'high'})
        theInterpolatedValues.append({'pm25': interpolatedEstimateHigh, 'time': estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'), 'origin': 'high'})

    LOGGER.info('the lowEstimates')
    LOGGER.info(lowEstimates.count())
    for estimateSliceLow in lowEstimates:
        # LOGGER.info(estimateSliceLow)
        estimationDateSliceDateLow = estimateSliceLow['estimationFor']
        # theDates.append({'date': estimationDateSliceDateLow, 'origin': 'low'})
        LOGGER.info(estimationDateSliceDateLow)

        leftBottomCorner_pm25valueLow = estimateSliceLow['estimate'][leftBottomCorner_index]['pm25']
        rightBottomCorner_pm25valueLow = estimateSliceLow['estimate'][rightBottomCorner_index]['pm25']
        leftTopCorner_pm25valueLow = estimateSliceLow['estimate'][leftTopCorner_index]['pm25']
        rightTopCorner_pm25valueLow = estimateSliceLow['estimate'][rightTopCorner_index]['pm25']

        Q11 = leftBottomCorner_pm25valueLow
        Q21 = rightBottomCorner_pm25valueLow
        Q12 = leftTopCorner_pm25valueLow
        Q22 = rightTopCorner_pm25valueLow

        interpolatedEstimateLow = bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2)

        # theInterpolatedValues.append({'lat': y, 'lng': x, 'pm25': interpolatedEstimateLow, 'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'contour': estimateSliceLow['contours'], 'origin': 'low'})
        theInterpolatedValues.append({'pm25': interpolatedEstimateLow, 'time': estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'), 'origin': 'low'})

        LOGGER.info('done with lowEstimates')

    LOGGER.info(theCorners)

    resp = jsonify(theInterpolatedValues)
    resp.status_code = 200

    LOGGER.info('*********** getting getEstimatesForLocation request done ***********')

    return resp


# too much data
@influx.route('/api/getGridEstimates', methods=['GET'])
def getGridEstimates():
    # needs only the timespan

    LOGGER.info('*********** getGridEstimates started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    startDate = queryParameters['start']
    startDate = datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%SZ')
    endDate = queryParameters['end']
    endDate = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')

    LOGGER.info('the start date')
    LOGGER.info(startDate)
    LOGGER.info('the end date')
    LOGGER.info(endDate)

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

    LOGGER.info(gridInfo)

    if gridInfo is not None:
        theGrid = gridInfo['transformedGrid']

        numberGridCells_LAT = gridInfo['numberOfGridCells']['lat']
        numberGridCells_LONG = gridInfo['numberOfGridCells']['long']

        LOGGER.info(theGrid)
        LOGGER.info(numberGridCells_LAT)
        LOGGER.info(numberGridCells_LONG)

        topRightCornerIndex = str((int(numberGridCells_LAT + 1) * int(numberGridCells_LONG + 1)) - 1)
        bottomLeftCornerIndex = str(0)
    else:
        LOGGER.info('grid info is none!')

    # get the 4 corners for each timestamp between the timespan
    # take all estimates in timeSpan

    # first take estimates from high collection
    # then estimates from low collection
    allHighEstimates = db.timeSlicedEstimates_high.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', 1)
    lowEstimates = db.timeSlicedEstimates_low.find({"estimationFor": {"$gte": startDate, "$lt": endDate}}).sort('estimationFor', 1)

    theGridValuesOverTime = []  # [{theGrid},{timestamp:{}}, ...]

    # append the grid info, each lat long corresponds to a gridID
    theGridInfo = {}
    for aGridID in range(int(bottomLeftCornerIndex), int(topRightCornerIndex) + 1):
        stringyfiedGridID = str(aGridID)
        aLat = theGrid[stringyfiedGridID]['lat'][0]
        aLng = theGrid[stringyfiedGridID]['lngs'][0]
        theGridInfo[aGridID] = {'lat': aLat, 'lng': aLng}

    theGridValuesOverTime.append(theGridInfo)

    LOGGER.info('the lowEstimates')
    LOGGER.info(lowEstimates.count())
    for estimateSliceLow in lowEstimates:
        estimationDateSliceDateLow = estimateSliceLow['estimationFor']
        LOGGER.info(estimationDateSliceDateLow)

        pm25s = []
        variability = []
        LOGGER.debug(int(bottomLeftCornerIndex))
        LOGGER.debug(int(topRightCornerIndex) + 1)
        for aGridID in range(int(bottomLeftCornerIndex), int(topRightCornerIndex) + 1):
            stringyfiedGridID = str(aGridID)
            LOGGER.debug(aGridID)
            aPm25 = estimateSliceLow['estimate'][stringyfiedGridID]['pm25']
            LOGGER.debug(aPm25)
            pm25s.append(aPm25)
            aVariability = estimateSliceLow['estimate'][stringyfiedGridID]['variability']
            LOGGER.debug(aVariability)
            variability.append(aVariability)

            # aGridElement = {'pm25': aPm25, 'variability': aVariability, 'gridID': aGridID}
            # LOGGER.debug(aGridElement)
            # theGridValues.append(aGridElement)

        gridDataForTimestamp_low = {'pm25': pm25s, 'variability': variability}
        theGridValuesOverTime.append({estimationDateSliceDateLow.strftime('%Y-%m-%dT%H:%M:%SZ'): gridDataForTimestamp_low})

        LOGGER.info('done with lowEstimates')

    LOGGER.info('the allHighEstimates')
    for estimateSliceHigh in allHighEstimates:
        estimationDateSliceDateHigh = estimateSliceHigh['estimationFor']

        # theGridValues = []
        pm25s = []
        variability = []
        for aGridID in range(int(bottomLeftCornerIndex), int(topRightCornerIndex) + 1):
            stringyfiedGridID = str(aGridID)
            aPm25 = estimateSliceHigh['estimate'][stringyfiedGridID]['pm25']
            pm25s.append(aPm25)
            aVariability = estimateSliceHigh['estimate'][stringyfiedGridID]['variability']
            variability.append(aVariability)

            # aGridElement = {'pm25': aPm25, 'variability': aVariability, 'gridID': aGridID}
            # theGridValues.append(aGridElement)
        gridDataForTimestamp_high = {'pm25': pm25s, 'variability': variability}
        theGridValuesOverTime.append({estimationDateSliceDateHigh.strftime('%Y-%m-%dT%H:%M:%SZ'): gridDataForTimestamp_high})

    resp = jsonify(theGridValuesOverTime)
    resp.status_code = 200

    LOGGER.info('*********** getting getGridEstimates request done ***********')

    return resp


@influx.route('/api/sensorsAtTime', methods=['GET'])
# where <sensorSource> is 'purpleAir', 'airU', or 'all'.
# where <selectedTime> is a string formatted like 2019-01-04T22:00:00Z
def getSensorsAtSelectTime():
    """Get sensors that are active around the time in the selectedTime datetime string"""

    LOGGER.info('*********** getSensorsAtSelectTime request started ***********')

    queryParameters = request.args
    LOGGER.info(queryParameters)

    sensorSource = queryParameters['sensorSource']
    selectedTime = queryParameters['selectedTime']

    if sensorSource not in ['purpleAir', 'airU', 'all']:
        LOGGER.info('sensorSource parameter is wrong')
        # abort(404)
        InvalidUsage('sensorSource parameter is wrong', status_code=404)

    selectedTimeStop = datetime.strptime(selectedTime, '%Y-%m-%dT%H:%M:%SZ')
    LOGGER.info('here 1')

    # Calculates the start of the time periods
    selectedTime3hStart = selectedTimeStop - timedelta(hours=3)
    LOGGER.info('here 2')
    selectedTime20mStart = selectedTimeStop - timedelta(minutes=20)
    selectedTime5mStart = selectedTimeStop - timedelta(minutes=5)

    # Formats each time into a string
    selectedTime3hStartStr = selectedTime3hStart.strftime('%Y-%m-%dT%H:%M:%SZ')
    selectedTime20mStartStr = selectedTime20mStart.strftime('%Y-%m-%dT%H:%M:%SZ')
    selectedTime5mStartStr = selectedTime5mStart.strftime('%Y-%m-%dT%H:%M:%SZ')
    selectedTimeStopStr = selectedTimeStop.strftime('%Y-%m-%dT%H:%M:%SZ')

    dataSeries = []
    start = time.time()

    LOGGER.info('string conversion done')

    if sensorSource == 'purpleAir':

        # get sensors that have pushed data to the db during the 5 minutes before the entered time
        dataSeries_purpleAir = getInfluxPollingSensorsSelectTime(selectedTime5mStartStr, selectedTimeStopStr, "Purple Air")
        LOGGER.info('length of dataSeries_purpleAir is {}'.format(len(dataSeries_purpleAir)))

        dataSeries_mesowest = getInfluxPollingSensorsSelectTime(selectedTime20mStartStr, selectedTimeStopStr, "Mesowest")
        LOGGER.info('length of dataSeries_mesowest is {}'.format(len(dataSeries_mesowest)))

        dataSeries_DAQ = getInfluxPollingSensorsSelectTime(selectedTime3hStartStr, selectedTimeStopStr, "DAQ")
        LOGGER.info('length of dataSeries_DAQ is {}'.format(len(dataSeries_DAQ)))

        dataSeries = dataSeries_purpleAir + dataSeries_mesowest + dataSeries_DAQ
        LOGGER.info('length of merged dataSeries is {}'.format(len(dataSeries)))

    elif sensorSource == 'airU':

        # get sensors that have pushed data to the db during the last 5min
        dataSeries = getInfluxAirUSensorsSelectTime(selectedTime5mStartStr, selectedTimeStopStr)
        LOGGER.info(len(dataSeries))

    elif sensorSource == 'all':

        # get sensors that have pushed data to the db during the last 5min
        LOGGER.info('get all dataSeries started')

        dataSeries_purpleAir = getInfluxPollingSensorsSelectTime(selectedTime5mStartStr, selectedTimeStopStr, "Purple Air")
        LOGGER.info('length of dataSeries_purpleAir is {}'.format(len(dataSeries_purpleAir)))

        dataSeries_mesowest = getInfluxPollingSensorsSelectTime(selectedTime20mStartStr, selectedTimeStopStr, "Mesowest")
        LOGGER.info('length of dataSeries_mesowest is {}'.format(len(dataSeries_mesowest)))

        dataSeries_DAQ = getInfluxPollingSensorsSelectTime(selectedTime3hStartStr, selectedTimeStopStr, "DAQ")
        LOGGER.info('length of dataSeries_DAQ is {}'.format(len(dataSeries_DAQ)))

        airUDataSeries = getInfluxAirUSensorsSelectTime(selectedTime5mStartStr, selectedTimeStopStr)
        LOGGER.info(len(airUDataSeries))
        LOGGER.debug(airUDataSeries)

        dataSeries = dataSeries_purpleAir + dataSeries_mesowest + dataSeries_DAQ + airUDataSeries
        LOGGER.info(len(dataSeries))
        LOGGER.debug(dataSeries)

        # from https://stackoverflow.com/questions/38279269/python-comparing-each-item-of-a-list-to-every-other-item-in-that-list by Kevin
        lats = dict()
        for idx, sensor in enumerate(dataSeries):
            if sensor['Latitude'] in lats:
                lats[sensor['Latitude']].append(idx)
            else:
                lats[sensor['Latitude']] = [idx]

        for lat, sameLats in lats.items():
            if len(sameLats) > 1:
                for i in range(len(sameLats)):
                    for j in range(i + 1, len(sameLats)):
                        if dataSeries[sameLats[i]]['ID'] != dataSeries[sameLats[j]]['ID'] and dataSeries[sameLats[i]]['Longitude'] == dataSeries[sameLats[j]]['Longitude']:
                            dataSeries[sameLats[j]]['Longitude'] = str(float(dataSeries[sameLats[j]]['Longitude']) - 0.0005)

        LOGGER.info('get all dataSeries done')
    else:
        LOGGER.info('wrong path is not catched')
        print('wrong path is not catched')
        # abort(404)
        InvalidUsage('wrong path is not catched', status_code=404)

    end = time.time()

    print("*********** Time to download:", end - start)

    resp = jsonify(dataSeries)
    resp.status_code = 200

    return resp


@influx.route('/api/macToBatch', methods=['POST'])
# where <sensorSource> is 'purpleAir', 'airU', or 'all'.
# where <selectedTime> is a string formatted like 2019-01-04T22:00:00Z
def getBatchForMac():

    print(request.is_json)
    content = request.get_json()
    print(content)

    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb

    theMappings = {}
    for aMapping in db.mappingMACToSensorID.find():
        # if aSensor['macAddress']:
        aMac = aMapping['macAddress']
        aSensorID = aMapping['customSensorID']

        aMapping = theMappings.get(aMac)
        if aMapping is not None:
            theMappings[aMac].append(aSensorID)
        else:
            theMappings[aMac] = [aSensorID]

    batchBoundary = 155
    inBothBatches = []
    batch1 = []
    batch2 = []
    batchAssignment = {}
    for aMac in content['mac']:
        aMacWithColon = aMac[0:2] + ':' + aMac[2:4] + ':' + aMac[4:6] + ':' + aMac[6:8] + ':' + aMac[8:10] + ':' + aMac[10:12]

        if theMappings.get(aMacWithColon) is not None:
            # check if there is a mapping, if no mapping then don't consider that data
            for aSensorID in theMappings[aMacWithColon]:

                sensorID = int(aSensorID.split('-')[2])

                if sensorID >= batchBoundary:
                    # batch 2

                    if aMac not in batch1 and aMac not in batch2:
                        batch2.append(aMac)
                        batchAssignment[aMac] = 'batch2'
                    elif aMac in batch1 and aMac not in batch2:
                        batch2.append(aMac)
                        inBothBatches.append(aMac)
                        batchAssignment[aMac] = 'bothBatch'

                elif sensorID < batchBoundary:
                    # batch 1

                    if aMac not in batch2 and aMac not in batch1:
                        batch1.append(aMac)
                        batchAssignment[aMac] = 'batch1'
                    elif aMac in batch2 and aMac not in batch1:
                        batch1.append(aMac)
                        inBothBatches.append(aMac)
                        batchAssignment[aMac] = 'bothBatch'

    resp = jsonify(batchAssignment)
    resp.status_code = 200

    return resp


# HELPER FUNCTIONS


def createSelection(typeOfQuery, querystring):
    """Creates the selection string for the SELECT statement."""

    LOGGER.info(querystring)

    addTags = False
    tags = ['altitude', 'id', 'latitude', 'longitude', 'sensor_model', 'sensor_source', 'sensor_version', 'start']
    tagString = ','.join(list(map(lambda x: lookupQueryParameterToInflux.get(x), tags)))

    if typeOfQuery == 'raw':

        show = querystring['show'].split(',')

        if 'meta' in show:
            addTags = True

        # create the selection string
        if 'all' in show:
            selectString = "*"
        else:
            # selectString = 'ID, \"SensorModel\", \"Sensor Source\"'
            selectString = ''
            for aShow in show:
                showExists = lookupQueryParameterToInflux.get(aShow)

                if aShow != 'id' and showExists is not None:
                    if aShow == 'pm25':
                        showExists = showExists + ' AS pm25'

                    # selectString = selectString + ", " + showExists + ", "
                    selectString = selectString + (', ' if selectString != '' else '') + showExists
                # else:
                    # TODO have an error message coming for this

        if addTags:
            selectString = selectString + ', ' + tagString

    elif typeOfQuery == 'processed':
        argument = querystring['functionArg']
        theDB = argument

        if querystring['sensorSource'] != 'airu':
            argumentExists = lookupQueryParameterToInflux.get(argument)
            theDB = 'airQuality'
        else:
            argumentExists = lookupParameterToAirUInflux.get(argument)

        LOGGER.info(theDB)
        LOGGER.info(argumentExists)

        if argumentExists is not None:
            alias = ' '
            if argument == 'pm25':
                alias = alias + 'AS pm25 '

            selectString = querystring['function'] + "(" + argumentExists + ")" + alias + 'FROM ' + theDB

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
            # liveAirUs.append({'macAddress': ''.join(aSensor['macAddress'].split(':')), 'registeredAt': aSensor['createdAt']})
            liveAirUs.append({'macAddress': ''.join(aSensor['macAddress'].split(':'))})

    return liveAirUs


def getMacToCustomSensorID():

    LOGGER.info('******** getMacToCustomSensorID started ********')
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    macToCustomID = {}

    # only use sensor that have been added to the macToCustomSensorID collection
    for aSensor in db.macToCustomSensorID.find():
        LOGGER.debug(aSensor)
        theMAC = ''.join(aSensor['macAddress'].split(':'))
        macToCustomID[theMAC] = aSensor['customSensorID']
        LOGGER.debug('sensor ID: {} and corresponding MAC address: {}'.format(aSensor['customSensorID'], theMAC))

    LOGGER.info(macToCustomID)

    LOGGER.info('******** getMacToCustomSensorID done ********')
    return macToCustomID


def getCustomSensorIDToMAC():

    LOGGER.info('******** getMacToCustomSensorIDToMac STARTED ********')
    mongodb_url = 'mongodb://{user}:{password}@{host}:{port}/{database}'.format(
        user=current_app.config['MONGO_USER'],
        password=current_app.config['MONGO_PASSWORD'],
        host=current_app.config['MONGO_HOST'],
        port=current_app.config['MONGO_PORT'],
        database=current_app.config['MONGO_DATABASE'])

    mongoClient = MongoClient(mongodb_url)
    db = mongoClient.airudb
    customIDToMAC = {}

    # only use sensor that have been added to the macToCustomSensorID collection
    for aSensor in db.macToCustomSensorID.find():
        LOGGER.debug(aSensor)
        theMAC = ''.join(aSensor['macAddress'].split(':'))
        customIDToMAC[aSensor['customSensorID']] = theMAC
        LOGGER.debug('sensor ID: {} and corresponding MAC address: {}'.format(aSensor['customSensorID'], theMAC))

    LOGGER.info('******** getMacToCustomSensorIDToMac DONE ********')
    return customIDToMAC


# https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
def mergeTwoDicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def getInfluxPollingSensors(aDateStr, sensorSource):

    LOGGER.info('******** influx polling started ********')

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_POLLING_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    queryInflux = "SELECT ID, \"Sensor Source\", Latitude, Longitude, LAST(\"pm2.5 (ug/m^3)\") AS pm25, \"Sensor Model\" " \
                  "FROM airQuality WHERE time >= '" + aDateStr + "' and \"Sensor Source\" = '" + sensorSource + "' " \
                  "GROUP BY ID, Latitude, Longitude, \"Sensor Source\"" \
                  "LIMIT 400"

    LOGGER.info(queryInflux)

    # start = time.time()
    data = influxClientPolling.query(queryInflux, epoch='ms')
    data = data.raw

    dataSeries = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), data['series']))
    # print(dataSeries)

# TODO THIS NEEDS TO BE IMPROVED in an easier way
    # # locating the double/parallel sensors (sensor with same location) and slightly changing the second sensors location, results in both dots visible
    # for i in range(len(dataSeries)):
    #     # LOGGER.info('i is %s', i)
    #     for j in range(i + 1, len(dataSeries)):
    #         # LOGGER.info('j is %s', j)
    #         # LOGGER.info('dataSeries[i] is %s', dataSeries[i])
    #         # LOGGER.info('dataSeries[j] is %s', dataSeries[j])
    #         if dataSeries[i]['ID'] != dataSeries[j]['ID']:
    #             if dataSeries[i]['Latitude'] == dataSeries[j]['Latitude'] and dataSeries[i]['Longitude'] == dataSeries[j]['Longitude']:
    #                 dataSeries[j]['Longitude'] = str(float(dataSeries[j]['Longitude']) - 0.0005)

    LOGGER.info(dataSeries)

    LOGGER.info('******** influx polling done ********')

    return dataSeries


def getInfluxAirUSensors(aDateStr):

    LOGGER.info('******** influx airU started ********')

    dataSeries = []

    liveAirUs = getAllCurrentlyLiveAirUs()  # call to mongodb
    LOGGER.info(len(liveAirUs))
    LOGGER.debug(liveAirUs)

    influxClientAirU = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                      port=current_app.config['INFLUX_PORT'],
                                      username=current_app.config['INFLUX_USERNAME'],
                                      password=current_app.config['INFLUX_PASSWORD'],
                                      database=current_app.config['INFLUX_AIRU_DATABASE'],
                                      ssl=current_app.config['SSL'],
                                      verify_ssl=current_app.config['SSL'])

    macToCustomID = getMacToCustomSensorID()
    LOGGER.info(macToCustomID)

    for airU in liveAirUs:
        LOGGER.debug(airU)

        macAddress = airU['macAddress']
        LOGGER.debug(macAddress)

        LOGGER.info('started get latitude')
        queryInfluxAirU_lat = "SELECT MEAN(Latitude) " \
                              "FROM " + current_app.config['INFLUX_AIRU_LATITUDE_MEASUREMENT'] + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + aDateStr + "'"

        LOGGER.debug(queryInfluxAirU_lat)

        dataAirU_lat = influxClientAirU.query(queryInfluxAirU_lat, epoch='ms')
        dataAirU_lat = dataAirU_lat.raw
        LOGGER.debug(dataAirU_lat)

        if 'series' not in dataAirU_lat:
            LOGGER.info('{} missing latitude'.format(macAddress))
            continue

        avgLat = dataAirU_lat['series'][0]['values'][0][1]
        LOGGER.info('finished get latitude')

        LOGGER.info('started get longitude')

        queryInfluxAirU_lng = "SELECT MEAN(Longitude) " \
                              "FROM " + current_app.config['INFLUX_AIRU_LONGITUDE_MEASUREMENT'] + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + aDateStr + "' "

        LOGGER.debug(queryInfluxAirU_lng)

        dataAirU_lng = influxClientAirU.query(queryInfluxAirU_lng, epoch='ms')
        dataAirU_lng = dataAirU_lng.raw
        LOGGER.debug(dataAirU_lng)

        if 'series' not in dataAirU_lng:
            LOGGER.info('{} missing longitude'.format(macAddress))
            continue

        avgLng = dataAirU_lng['series'][0]['values'][0][1]
        LOGGER.info('finished get longitude')

        LOGGER.info('started get latest pm25 measurement')

        queryInfluxAirU_lastPM25 = "SELECT LAST(" + lookupParameterToAirUInflux.get('pm25') + ") AS pm25, ID " \
                                   "FROM " + current_app.config['INFLUX_AIRU_PM25_MEASUREMENT'] + " "\
                                   "WHERE ID = '" + macAddress + "' "

        LOGGER.debug(queryInfluxAirU_lastPM25)

        dataAirU_lastPM25 = influxClientAirU.query(queryInfluxAirU_lastPM25, epoch='ms')
        dataAirU_lastPM25 = dataAirU_lastPM25.raw

        LOGGER.debug(dataAirU_lastPM25)

        if 'series' not in dataAirU_lastPM25:
            LOGGER.info('{} missing lastPM25'.format(macAddress))
            continue

        lastPM25 = dataAirU_lastPM25['series'][0]['values'][0][1]
        pm25time = dataAirU_lastPM25['series'][0]['values'][0][0]

        LOGGER.info('finished get latest pm25 measurement')

        newID = macAddress
        if macAddress in macToCustomID:
            newID = macToCustomID[macAddress]

            LOGGER.debug('newID is {}'.format(newID))

            anAirU = {'ID': newID, 'Latitude': str(avgLat), 'Longitude': str(avgLng), 'Sensor Source': 'airu', 'pm25': lastPM25, 'time': pm25time}
            # anAirU = {'ID': airU['macAddress'], 'Latitude': str(avgLat), 'Longitude': str(avgLng), 'Sensor Source': 'airu', 'pm25': lastPM25, 'time': pm25time}
            LOGGER.debug(anAirU)

            dataSeries.append(anAirU)
            LOGGER.debug('airU appended')

    LOGGER.debug(dataSeries)
    LOGGER.info('******** influx airU done ********')

    return dataSeries


# interpolation function
def bilinearInterpolation(Q11, Q12, Q21, Q22, x, y, x1, x2, y1, y2):

    LOGGER.info('bilinearInterpolation')
    # f(x,y) = 1/((x2 - x1)(y2 - y1)) * (f(Q11) * (x2 - x)(y2 - y) + f(Q21) * (x - x1)(y2 - y) + f(Q12) * (x2 - x)(y - y1) + f(Q22) * (x - x1)(y - y1)
    LOGGER.debug('Q11 is %s', Q11)
    LOGGER.debug('Q12 is %s', Q12)
    LOGGER.debug('Q21 is %s', Q21)
    LOGGER.debug('Q22 is %s', Q22)
    LOGGER.debug('x is %s', x)
    LOGGER.debug('y is %s', y)
    LOGGER.debug('y1 is %s', y1)
    LOGGER.debug('y2 is %s', y2)
    LOGGER.debug('x1 is %s', x1)
    LOGGER.debug('x2 is %s', x2)

    LOGGER.debug(type(Q11))
    LOGGER.debug(type(Q12))
    LOGGER.debug(type(Q21))
    LOGGER.debug(type(Q22))
    LOGGER.debug(type(x))
    LOGGER.debug(type(y))
    LOGGER.debug(type(y1))
    LOGGER.debug(type(y2))
    LOGGER.debug(type(x1))
    LOGGER.debug(type(x2))

    interpolatedValue = 1.0 / ((x2 - x1) * (y2 - y1)) * ((Q11 * (x2 - x) * (y2 - y)) + (Q21 * (x - x1) * (y2 - y)) + (Q12 * (x2 - x) * (y - y1)) + (Q22 * (x - x1) * (y - y1)))

    LOGGER.info('interpolatedValue is %s', interpolatedValue)

    return interpolatedValue


def FloorTimestamp2Minute(df):
    """
    Floor dataframe Influx Timestamps to the minute
    and turn them into a string. Then replace the
    timestamps in <df> index with these

    :param df: The Pandas DataFrame with Pandas Timestamp as index
    :return: <df> with updated index (index is string representation of timestamp with minute precision)
    """

    LOGGER.info('********** FloorTimestamp2Minute **********')
    try:
        df.index = [t.round('T') for t in df.index.tolist()]
    except AttributeError as e:
        LOGGER.error('FloorTimestamp2Minute: {}'.format(str(e)))
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
    LOGGER.info('********** AddSeries2Dataframe **********')

    # truncate the timestamps to minute precision
    LOGGER.info('calling FloorTimestamp2Minute on dataframe')
    df = FloorTimestamp2Minute(df_single_series)

    # Create a new dataframe with the sensor name as the column title
    #   and the measurement as the data. Index is time (minute precision)
    LOGGER.info('Creating new dataframe with sensor={} and Field Key={}'.format(sensor_name, measurement_fieldKey))
    df = pd.DataFrame({sensor_name: df[measurement_fieldKey]}, index=df.index)

    # First time adding a sensor this function should be passed an empty
    #   DataFrame. Just return it. Otherwise join the two into one
    if df_combined.empty:
        LOGGER.info('new dataframe passed')
        return df
    else:

        LOGGER.info('Joining dataframes')
        df_combined = df_combined.join(df, how='outer')

        # Sometimes there are multiple measurements with the same timestamp after
        #   truncating to the minute. Just delete the second one. To hell with it
        LOGGER.info('Removing duplicate indices from combined dataframe')
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

    LOGGER.info('********** getSensorSource({}) **********'.format(sensorID))

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
            LOGGER.error('getSensorSource: {}'.format(str(e)))
            return False


def getInfluxPollingSensorsSelectTime(aDateStart, aDateEnd, sensorSource):

    LOGGER.info('******** influx SelectTime polling started ********')

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_POLLING_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    queryInflux = "SELECT ID, \"Sensor Source\", Latitude, Longitude, LAST(\"pm2.5 (ug/m^3)\") AS pm25, \"Sensor Model\" " \
                  "FROM airQuality WHERE time >= '" + aDateStart + "' and time <= '" + aDateEnd + "' and \"Sensor Source\" = '" + sensorSource + "' " \
                  "GROUP BY ID, Latitude, Longitude, \"Sensor Source\"" \
                  "LIMIT 400"

    LOGGER.info(queryInflux)

    # start = time.time()
    data = influxClientPolling.query(queryInflux, epoch='ms')
    data = data.raw

    dataSeries = list(map(lambda x: dict(zip(x['columns'], x['values'][0])), data['series']))

    LOGGER.info(dataSeries)

    LOGGER.info('******** influx SelectTime polling done ********')

    return dataSeries


def getInfluxAirUSensorsSelectTime(aDateStart, aDateStop):

    LOGGER.info('******** influx SelectTime airU started ********')

    LOGGER.info(aDateStart)
    LOGGER.info(aDateStop)

    dataSeries = []

    liveAirUs = getAllCurrentlyLiveAirUs()  # call to mongodb
    LOGGER.info(len(liveAirUs))
    LOGGER.debug(liveAirUs)

    influxClientPolling = InfluxDBClient(host=current_app.config['INFLUX_HOST'],
                                         port=current_app.config['INFLUX_PORT'],
                                         username=current_app.config['INFLUX_USERNAME'],
                                         password=current_app.config['INFLUX_PASSWORD'],
                                         database=current_app.config['INFLUX_AIRU_DATABASE'],
                                         ssl=current_app.config['SSL'],
                                         verify_ssl=current_app.config['SSL'])

    macToCustomID = getMacToCustomSensorID()
    LOGGER.info(macToCustomID)

    for airU in liveAirUs:
        LOGGER.debug(airU)

        macAddress = airU['macAddress']
        LOGGER.debug(macAddress)

        LOGGER.info('started get latitude')
        queryInfluxAirU_lat = "SELECT MEAN(Latitude) " \
                              "FROM " + 'latitude' + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + aDateStart + "' and time <= '" + aDateStop + "'"

        LOGGER.info(queryInfluxAirU_lat)

        dataAirU_lat = influxClientPolling.query(queryInfluxAirU_lat, epoch='ms')

        dataAirU_lat = dataAirU_lat.raw
        LOGGER.info(dataAirU_lat)

        if 'series' not in dataAirU_lat:
            LOGGER.info('{} missing latitude'.format(macAddress))
            continue

        avgLat = dataAirU_lat['series'][0]['values'][0][1]
        LOGGER.info('finished get latitude')

        LOGGER.info('started get longitude')

        queryInfluxAirU_lng = "SELECT MEAN(Longitude) " \
                              "FROM " + 'longitude' + " "\
                              "WHERE ID = '" + macAddress + "' and time >= '" + aDateStart + "' and time <= '" + aDateStop + "'"

        LOGGER.debug(queryInfluxAirU_lng)

        dataAirU_lng = influxClientPolling.query(queryInfluxAirU_lng, epoch='ms')
        dataAirU_lng = dataAirU_lng.raw
        LOGGER.debug(dataAirU_lng)

        if 'series' not in dataAirU_lng:
            LOGGER.info('{} missing longitude'.format(macAddress))
            continue

        avgLng = dataAirU_lng['series'][0]['values'][0][1]

        LOGGER.info('finished get longitude')

        LOGGER.info('started get latest pm25 measurement')

        queryInfluxAirU_lastPM25 = "SELECT LAST(" + lookupParameterToAirUInflux.get('pm25') + ") AS pm25, ID " \
                                   "FROM " + "pm25" + " "\
                                   "WHERE ID = '" + macAddress + "' and time >= '" + aDateStart + "' and time <= '" + aDateStop + "'"

        LOGGER.debug(queryInfluxAirU_lastPM25)

        dataAirU_lastPM25 = influxClientPolling.query(queryInfluxAirU_lastPM25, epoch='ms')
        dataAirU_lastPM25 = dataAirU_lastPM25.raw

        LOGGER.debug(dataAirU_lastPM25)

        if 'series' not in dataAirU_lastPM25:
            LOGGER.info('{} missing lastPM25'.format(macAddress))
            continue

        lastPM25 = dataAirU_lastPM25['series'][0]['values'][0][1]
        pm25time = dataAirU_lastPM25['series'][0]['values'][0][0]

        LOGGER.info('finished get latest pm25 measurement')
        newID = macAddress + "AIR U"
        if macAddress in macToCustomID:
            newID = macToCustomID[macAddress]

            LOGGER.debug('newID is {}'.format(newID))

            anAirU = {'ID': newID, 'Latitude': str(avgLat), 'Longitude': str(avgLng), 'Sensor Source': 'airu', 'pm25': lastPM25, 'time': pm25time}

            LOGGER.debug(anAirU)

            dataSeries.append(anAirU)
            LOGGER.debug('airU appended')

    LOGGER.info(dataSeries)
    LOGGER.info('******** influx SelectTime airU done ********')

    return dataSeries
