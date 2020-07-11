import logging
import sqlite3
import pandas as pd
import shutil
import os


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

        try:
            logging.info(
                'Reading Database -  Reading HomeAssistan Database File')
            self.df_db = read_db(self.path)
        except Exception as e:  # skipcq: PYL-W0703
            logging.error("Error Reading Database - Reading database file")
            logging.error(e)

        try:
            if self.df_csv.empty:
                self.df_csv = self.df_db
            else:
                self.df_csv = self.df_csv.combine_first(self.df_db)
        except Exception as e:  # skipcq: PYL-W0703
            logging.error(
                "Error Reading Database - Error combining DataFrames")
            logging.error(e)

        self.create_statistics()

    def create_statistics(self):

        df = self.df_csv
        txt = 'HomeAssistant Values Summary\n'

        for x in df['entity_id'].unique():
            no_sensor_values = "False" in str(
                df[df['entity_id'] == x]['float'].value_counts().to_dict())
            if (x[:7] == "sensor.") and not no_sensor_values:
                txt += 'Entity: %s \n' % x
                df_lv = df[df['entity_id'] == x].tail(
                    self.statistics_sample)
                txt += 'First Value: %s \n' % str(
                    df_lv['created'].to_list()[0])
                txt += 'Last Value:%s \n ' % str(
                    df_lv['created'].to_list()[-1])
                txt += 'Min: %2.2f \n' % df_lv['state'].min()
                txt += 'Mean: %2.2f \n' % df_lv['state'].mean()
                txt += 'Max: %2.2f \n' % df_lv['state'].max()

        self.email_txt = txt
