import logging
import sqlite3
import pandas as pd
import shutil
import os
from datetime import datetime


def convert_to_float(x):
    """ Convert value to float """
    try:
        return float(x)
    except Exception as e:  # skipcq: PYL-W0703
        str(e)
        return x


def read_db(db_filename):
    """ Read a database giving the path """

    tmp_db = 'database_tmp_file.db'
    # tmp_csv = 'csv_tmp_file.csv'

    try:
        shutil.copyfile(db_filename, tmp_db)
    except Exception as e:  # skipcq: PYL-W0703
        logging.error(e)

    con = sqlite3.connect(tmp_db)
    df = pd.read_sql_query("SELECT * FROM states;", con)
    df = df.drop(['domain', 'event_id', 'context_id',
                  'context_user_id', 'last_changed', 'last_updated'], axis=1)
    df = df.set_index('state_id')

    df = df[df['state'] != 'unknown']
    df['state'] = df['state'].apply(convert_to_float)
    df['float'] = df['state'].apply(lambda x: type(x) is float)

    # df.to_csv(tmp_csv)
    # df = pd.read_csv(tmp_csv, index_col='state_id')
    # df['state'] = df['state'].apply(lambda x: convert_to_float(x))

    os.remove(tmp_db)

    return df


def read_csv_db(csv_filename):
    """ Read a HomeAssistant CSV File """
    try:
        df = pd.read_csv(csv_filename, index_col='state_id')
        df['state'] = df['state'].apply(convert_to_float)
    except Exception as e:  # skipcq: PYL-W0703
        df = pd.DataFrame()
        logging.error(e)

    return df


class HADatabase:
    def __init__(self, path='', csv_path=''):
        """ Init HA Database Object"""

        logging.info('Creating HADatabase Object')
        self.path = path
        self.csv_path = csv_path
        self.df_csv = read_csv_db(self.csv_path)

        self.df_db = pd.DataFrame()

        self.email_txt = ''
        self.statistics_sample = 700

    def read_database(self):
        """ Read Database File in Path """

        # try:
        #     logging.info(
        #         'Reading Database -  Reading HomeAssistan Database File')
        #     self.df_db = read_db(self.path)
        # except Exception as e:  # skipcq: PYL-W0703
        #     logging.error("Error Reading Database - Reading database file")
        #     logging.error(e)

        # try:
        #     if self.df_csv.empty:
        #         self.df_csv = self.df_db
        #     else:
        #         self.df_csv = self.df_csv.combine_first(self.df_db)
        # except Exception as e:  # skipcq: PYL-W0703
        #     logging.error(
        #         "Error Reading Database - Error combining DataFrames")
        #     logging.error(e)

        self.create_statistics()

    def create_statistics(self):

        # df = self.df_csv
        # txt = 'HomeAssistant Values Summary\n'

        # for x in df['entity_id'].unique():
        #     no_sensor_values = "False" in str(
        #         df[df['entity_id'] == x]['float'].value_counts().to_dict())
        #     if (x[:7] == "sensor.") and not no_sensor_values:
        #         txt += 'Entity: %s \n' % x
        #         df_lv = df[df['entity_id'] == x].tail(
        #             self.statistics_sample)
        #         txt += 'First Value: %s \n' % str(
        #             df_lv['created'].to_list()[0])
        #         txt += 'Last Value:%s \n ' % str(
        #             df_lv['created'].to_list()[-1])
        #         txt += 'Min: %2.2f \n' % df_lv['state'].min()
        #         txt += 'Mean: %2.2f \n' % df_lv['state'].mean()
        #         txt += 'Max: %2.2f \n' % df_lv['state'].max()

        # self.email_txt = txt

        df = pd.read_csv('HA-Watch.log', sep=' ', skiprows=415268,
                         error_bad_lines=False, names=['Day', 'Time', 'Feed', 'Value'])
        df['Feed'] = df['Feed'].apply(lambda s: s[:-1])
        df['Time'] = df['Time'].apply(lambda s: s[:-1])

        df['Value'] = df['Value'].apply(lambda x: convert_to_float(x))

        df['Timestamp'] = df['Day'] + ' '+df['Time']
        df['Timestamp'] = pd.to_datetime(
            df['Timestamp'], format="%d-%m-%y %H:%M:%S")

        txt = 'HomeAssistant Values Summary\n\n'

        txt = 'Sensor Values\n\n'

        topics = ['bath/temperature', 'bath/humidity',
                  'kitchen/temperature', 'kitchen/humidity',
                  'living/temperature', 'living/humidity',
                  'pihumboldt/temperature', 'pitv/temperature']

        now = datetime.now()
        today_date = now.strftime("%d/%m/%Y %H:%M:%S")

        df_today = df[df["Timestamp"] >= (
            pd.to_datetime(today_date) - pd.Timedelta(hours=24))]

        min_t = df_today['Timestamp'].min()
        max_t = df_today['Timestamp'].max()

        txt += "From: %s\n" % (str(min_t))
        txt += "Until: %s\n\n" % (str(max_t))

        for topic in topics:
            try:

                min_v = df_today[df_today['Feed'].str.contains(
                    topic) & df_today['Value'] > 0]['Value'].min()
                min_t = df_today.loc[pd.to_numeric(df_today[df_today['Feed'].str.contains(
                    topic) & df_today['Value'] > 0]['Value']).idxmin()]['Timestamp']

                max_v = df_today[df_today['Feed'].str.contains(
                    topic) & df_today['Value'] > 0]['Value'].max()
                max_t = df_today.loc[pd.to_numeric(df_today[df_today['Feed'].str.contains(
                    topic) & df_today['Value'] > 0]['Value']).idxmax()]['Timestamp']

                mean_v = df_today[df_today['Feed'].str.contains(
                    topic) & df_today['Value'] > 0]['Value'].mean()

                if 'temperature' in topic:
                    unit = '°C'
                elif 'humidity' in topic:
                    unit = '%'
                else:
                    unit = ''

                txt += "%s\n" % (topic)
                txt += "Mean Value: %2.1f%s\n" % (mean_v, unit)
                txt += "Min: %2.1f%s (%s)\n" % (min_v, unit, str(min_t))
                txt += "Max: %2.1f%s (%s)\n\n" % (max_v, unit, str(max_t))
            except Exception as e:  # skipcq: PYL-W0703
                logging.error("Error evaluating Stats")
                logging.error(e)

        topics = ['bath/leak', 'kitchen/leak',
                  'living/pir', 'door/door-status']

        for topic in topics:
            try:
                txt += "%s\n" % (topic)
                txt += str(df_today[df_today['Feed'].str.contains(topic)
                                    & df_today['Value'] > 0]['Value'].value_counts())
                txt += '\n\n'
            except Exception as e:  # skipcq: PYL-W0703
                logging.error("Error evaluating Stats")
                logging.error(e)

        self.email_txt = txt
