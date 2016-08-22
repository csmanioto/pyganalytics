# coding: utf-8

import httplib2
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


# System libraries
import simplejson as json
import datetime, time, timeit
import socket, logging, re
import pandas as pd
import sqlalchemy #sudo pip3 install sqlalchemy-redshift


# Configurando logs
logger = logging.getLogger("google_analytics")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)

"""
    Google Analytics  Libraries
    https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
    https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/service-py
    https://developers.google.com/apis-explorer/#p/

    Install Google API for Python. :
        pip3 install -I google-api-python-client
        pip3 install --upgrade oauth2client
"""


class googleAnalytics():


    def getConfig(self, filename="setup.json", configName=None):
        with open(filename, 'r') as file:
            result = json.load(file)

        if configName is not None:
            result = result[configName]
        return result


    def __init__(self):
        setup = self.getConfig(filename='setup.json')

        self.apiVersion = setup['Config']['version']
        self.account_id = setup['Query']['account_id']
        self.account_name = setup['Query']['account_name']
        self.webpropertyid = setup['Query']['webPropertyId']
        self.profile = setup['Query']['profile']
        self.profile_id = setup['Query']['profile'].split(":")[1]

        if self.apiVersion == 'v4':
            self.serviceName = setup['Config']['serviceName_v4']
            discoveryServiceUrl = setup['Config']['discoveryServiceUrl_v4']
            self.samplingLevel = setup['Config']['samplingLevel_v4']

        if self.apiVersion == 'v3':
            self.serviceName = setup['Config']['serviceName_v3']
            self.discoveryServiceUrl = setup['Config']['discoveryServiceUrl_v3']
            self.discoveryLink = setup['Config']['discoveryLink_v3']
            self.samplingLevel = setup['Query']['samplingLevel_v3']

            self.service_account_email = setup['Config']['service_account_email']
            self.key_file_location = setup['Config']['key_file_location']
            self.scope = setup['Config']['scope']

    def __authenticate__(self):
        credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email=self.service_account_email,
                                                                 filename=self.key_file_location,
                                                                 scopes=self.scope)
        http = credentials.authorize(httplib2.Http())
        service = build(serviceName=self.serviceName, version=self.apiVersion, http=http,
                        discoveryServiceUrl=self.discoveryServiceUrl)
        logger.debug(service)
        return service

    def get_profiles(self, analyticsAPI, accountId=None, webPropertyId="~all", profileId=None):
        # [{"View_profileName": "1 VR_BR", "webPropertyId": "UA-126375-31", "View_profileId": "12906114", "AccountId": "126375"}]'
        if accountId is None:
            accounts = self.analytics.management().accounts().list().execute()
            for account in accounts.get('items', []):
                accountId = account.get('id')
                return self.get_profiles(accountId, webPropertyId=webPropertyId, profileId=profileId)

        else:
            profiles = analyticsAPI.management().profiles().list(accountId=accountId,
                                                                 webPropertyId=webPropertyId).execute()
            list_profiles = list()
            for profile in profiles.get('items', []):
                dic_profie = {'AccountId': profile.get('accountId'),
                              'webPropertyId': profile.get('webPropertyId'),
                              'View_profileId': profile.get('id'),
                              'View_profileName': profile.get('name')
                              }
                list_profiles.append(dic_profie)

            if profileId is not None:
                list_filtred = [item for item in list_profiles if item['View_profileId'] == profileId]
                list_profiles = list_filtred

            return list_profiles

    def formSamplingLevel(self, samplingLevel=None):
        defaulSamplingLevel = {"v3": "HIGHER_PRECISION", "v4": "LARGE"}
        if samplingLevel is None:
            if self.apiVersion == 'v4':
                samplingLevel = defaulSamplingLevel["v4"]
            if self.apiVersion == 'v3':
                samplingLevel = defaulSamplingLevel["v3"]

        # Convert sample Level to List
        if isinstance(samplingLevel, str):
            samplingLevel = samplingLevel.strip()

        return samplingLevel

    def queryGA(self, analyticsAPI, samplingLevel=None):
        setup = self.getConfig(filename="setup.json", configName="Query")
        startDate = str(setup['start']).strip()  # end
        endDate = str(setup['end']).strip()
        metrics = str(setup['metrics']).strip()
        dimensions = str(setup['dimensions']).strip()

        # [{"View_profileName": "1 VR_BR", "webPropertyId": "UA-126375-31", "View_profileId": "12906114", "AccountId": "126375"}]'
        list_profiles = self.get_profiles(analyticsAPI=analyticsAPI, accountId=self.account_id,
                                          webPropertyId=self.webpropertyid, profileId=self.profile_id, )

        # First Loop, interact over each profile(Account, profileID)
        # Second Loop, get data for each SamplingLevel for custom analyze.
        response = list()
        for profile in list_profiles:
            logger.debug("Query for profile {}".format(profile['View_profileId']))

            for level in self.formSamplingLevel(samplingLevel):
                logger.debug("  ->  samplingLevel {}".format(level))

                if self.apiVersion == 'v4':
                    dict_query_v4 = {
                        'reportRequests': [
                            {
                                'viewId': profile['View_profileId'],
                                'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                                'metrics': [{'expression': metrics}],
                                'dimensions': [{"name": dimensions}],
                                'samplingLevel': level,
                                'includeEmptyRows': 'true'
                            }
                        ]
                    }
                    json_query_v4 = json.dumps(dict_query_v4)
                    logger.debug(json_query_v4)
                    response.append(analyticsAPI.reports().batchGet(body=json_query_v4).execute())

                if self.apiVersion == 'v3':
                    response.append(analyticsAPI.data().ga().get(
                        ids='ga:{}'.format(profile['View_profileId']),
                        start_date=startDate,
                        end_date=endDate,
                        metrics=metrics,
                        dimensions=dimensions,
                        include_empty_rows=True,
                        samplingLevel=level).execute())

        return response

    def result2Pandas(self, results):
        ##
        # If empty return dataFrame empty
        if not results:
            return None

        colunmns = results[0]["columnHeaders"]

        pandas_columns = list()
        for coluna in colunmns:
            pandas_columns.append(str(coluna['name']).split(':')[1])

        pandas_columns.append("querysamplinglevel")
        pandas_columns.append("sampleddata")
        pandas_columns.append("samplesize")
        pandas_columns.append("samplespace")
        pandas_columns.append("sampling_perc")
        pandas_columns.append("accountid")
        pandas_columns.append("webpropertyid")
        pandas_columns.append("profileid")
        pandas_columns.append("tableid")

        googleDataFrame = pd.DataFrame(columns=pandas_columns)

        for row in range(0, len(results)):
            accountId = results[row].get('profileInfo')['accountId'],
            webPropertyId = results[row].get('profileInfo')['webPropertyId'],
            profileId = results[row].get('profileInfo')['profileId'],
            tableId = results[row].get('profileInfo')['tableId']
            querysamplinglevel = results[row].get('query')['samplingLevel']
            sampleddata_bool = bool(results[row].get('containsSampledData'))
            """"
                Se a resposta de uma API de relatórios principais contiver dados de amostra, o campo de resposta containsSampledData será true.
                Além disso, duas propriedades fornecerão informações sobre o nível de amostragem da consulta: sampleSize e sampleSpace.
                Com esses dois valores, é possível calcular a porcentagem de sessões que foram usadas para a consulta.
                Por exemplo, se sampleSize é 201.000 e sampleSpace é 220.000, o relatório se baseia em (201.000 / 220.000) * 100 = 91,36% das sessões.
            """
            sampling = 0.00
            sampleSize = 0.00
            sampleSpace = 0.00
            if sampleddata_bool:
                try:
                    sampleSize = results[row].get('sampleSize')
                    sampleSpace = results[row].get('sampleSpace')
                    sampling = round((float(sampleSize) / float(sampleSpace) * 100), 2)
                except Exception as e:
                    logger.debug("Error on get sampling metric: {}".format(e))

            date_index = None
            data_index = None
            preserie = [None] * len(pandas_columns)
            for data_index, data in enumerate(results[row].get('rows')):
                list_data = list(data)
                # print(list_data[1])
                # Format date value from YYYMMMDD to YYYY-MM-DD
                for index, value in enumerate(data):
                    # YYYYMMDD
                    if "date" in pandas_columns[index] and re.findall(r"\b\d{8}\b", list_data[index]):
                        date = str(datetime.datetime.strptime(list_data[index], '%Y%m%d').date())
                        date_index = index
                    else:
                        data_index = index

                preserie = [
                    str(date),
                    str(list_data[1]),
                    str(querysamplinglevel),
                    sampleddata_bool,
                    sampleSize,
                    sampleSpace,
                    sampling,
                    str(''.join(accountId)),
                    str(''.join(webPropertyId)),
                    str(''.join(profileId)),
                    str(tableId)
                ]
                logger.debug("Making pandas series {}".format(preserie))
                series = pd.Series(preserie, index=pandas_columns)
                googleDataFrame = googleDataFrame.append(series, ignore_index=True)

        googleDataFrame['date'] = pd.to_datetime(googleDataFrame.date)
        return googleDataFrame.sort_values(by='date')

    def getData(self):
        analyticsAPI = self.__authenticate__()
        result = self.queryGA(analyticsAPI=analyticsAPI, samplingLevel=self.samplingLevel)
        return self.result2Pandas(result)

        # http://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table

        # DataFrame.to_sql(table, engine)


    def pandas2PostgreSQL(self, DataFrame):
        try:
            dict_db = self.getConfig(configName="DBConfig")
            dbengine = dict_db['engine']
            user = dict_db['user']
            password = dict_db['password']
            endPoint = socket.gethostbyname_ex(dict_db['endPoint'])[0]
            tcpPort = dict_db['tcpPort']
            database = dict_db['database']
            table = dict_db['table']

            url = "{}://{}:{}@{}:{}/{}".format(dbengine, user, password, endPoint, tcpPort, database)
            logger.debug("DB URL connection: {}".format(url))
            engine = sqlalchemy.create_engine(url)
            meta = sqlalchemy.MetaData(engine, schema='dw')
            index_label = None
            index = False
            if not "redshift" in dbengine:
                index_label = 'data'
                index = True

            logger.debug("Inserting into talble {}".format(table))
            t0 = time.time()
            DataFrame.to_sql(name=table,
                             con=engine,
                             schema='dw',
                             if_exists='replace',
                             index=index,
                             index_label=index_label,
                             chunksize=100)
            t1 = time.time()
            time_running = round(t1 - t0, 2)

            return (True, time_running)
        except Exception as e:
            logger.debug("Error on insert into PostgreSQL: {}".format(e))
            print(e)
            return (False, e)

    def pandas2CSV(self,DataFrame):
        try:
            t0 = time.time()
            DataFrame.to_csv("google_analytics.csv", sep=";", encoding="utf-8", doublequote=True, header=True, index=False)
            t1 = time.time()
            time_running = round(t1 - t0, 2)

            return (True, time_running)
        except Exception as e:
            logger.debug("Error on export to csv: {}".format(e))
            return (False, e)

def main():
    ga = googleAnalytics()
    df = ga.getData()
    #returned = ga.pandas2CSV(df)
    returned = ga.pandas2PostgreSQL(df)
    if returned[0]:
        print("Importado no DB em {} segundos".format(returned[1]))
    else:
        print("Problema ao insert no banco...{} ".format(returned[1]))


if __name__ == '__main__':
    print(timeit.timeit("main()", setup="from __main__ import main"))