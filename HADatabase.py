import logging
import sqlite3
import pandas as pd
import shutil
import os


def convert_to_float(x):
    try:
        return(float(x))
    except Exception as e:  # skipcq: PYL-W0703
        return(x)


def read_db(db_filename):
    con = sqlite3.connect(db_filename)
    df = pd.read_sql_query("SELECT * FROM states;", con)
    df = df.drop(['domain', 'event_id', 'context_id',
                  'context_user_id', 'last_changed', 'last_updated'], axis=1)
    df = df.set_index('state_id')

    df = df[df['state'] != 'unknown']
    df['state'] = df['state'].apply(lambda x: convert_to_float(x))
    df['float'] = df['state'].apply(lambda x: type(x) == float)

    df.to_csv('home-assistant.csv')
    df = pd.read_csv('home-assistant.csv', index_col='state_id')
    df['state'] = df['state'].apply(lambda x: convert_to_float(x))

    return df


class HADatabase:
    def __init__(self, path=''):
        """ Init HA Database Object"""

        logging.info('Creating HADatabase - Just Assigning Path')

        self.con = 0
        self.battery_min = 0
        self.battery_mean = 0
        self.battery_max = 0
        self.df_csv = pd.DataFrame()
        self.df_db = pd.DataFrame()
        self.email_txt = ''
        self.csv_working_dir = ''
        self.db_working_dir = ''

        if path != '':
            self.path = path
        else:
            self.path = ''

    def read_database(self):
        """ Read Database File in Path """

        try:
            logging.info('Reading Database -  Copying DB to Working Dir')
            self.copy_db_to_working_dir()
            try:
                self.con = sqlite3.connect(self.db_working_dir)
                logging.info('Reading Database - Connecting Succeded!')
                try:
                    logging.info(
                        'Reading Database - Saving Data into Dataframe')
                    self.df_db = pd.read_sql_query('SELECT * FROM states;',
                                                   self.con)
                    self.con.close()
                    try:  # Then save the Dataframe into the csv to create statistics
                        logging.info(
                            'Reading Database - Creating Backup in CSV')
                        self.save_to_csv_db()
                        logging.info('Deleting DB File on working dir')
                        os.remove(self.db_working_dir)
                    except Exception as e:  # skipcq: PYL-W0703
                        logging.error(e)
                except Exception as e:  # skipcq: PYL-W0703
                    logging.info('Saving Data into Dataframe failed')
                    logging.error(e)
                    self.df_db = 0

            except Exception as e:  # skipcq: PYL-W0703
                logging.info('Connecting Failed!')
                logging.error('%s %s', e, self.path)
                self.con = 0
        except Exception as e:  # skipcq: PYL-W0703
            logging.error('Error Copying DB to Working Dir')
            logging.error('%s %s', e, self.path)

    def copy_db_to_working_dir(self):
        """ Copy DB File to Working dir """

        self.db_working_dir = 'data/home-assistant.db'
        try:
            shutil.copyfile(self.path, self.db_working_dir)
        except Exception as e:  # skipcq: PYL-W0703
            logging.error(e)

    def save_to_csv_db(self):
        """ Save File to CSV """

        self.csv_working_dir = 'data/home-assistant.csv'
        self.df_db.to_csv(self.csv_working_dir)
        # TO DO read previous CSV and append new information
        # TO DO Clean Database

    def prepare_email(self):
        """ Prepare E-Mail to be sent """

        self.email_txt = 'Good Morning! Home Assistant is Alive\n'

        self.df_csv = pd.read_csv(self.csv_working_dir)

        self.calculate_battery_values()

        self.email_txt += ('Battery Max: %2.2f  \n' % (self.battery_min))
        self.email_txt += ('Battery Min: %2.2f  \n' % (self.battery_max))
        self.email_txt += ('Battery Mean: %2.2f  \n' % (self.battery_mean))

    def calculate_battery_values(self):
        """ Temp function which calculates some values to be sent """
        try:
            x = self.df_csv[(
                self.df_csv['entity_id'] == 'sensor.battery_status_garden')]
            x = x.replace('unknown', method='bfill')['state'].astype(float)
            self.battery_min = x.min()
            self.battery_mean = x.mean()
            self.battery_max = x.max()
        except Exception as e:  # skipcq: PYL-W0703
            logging.error(e)
            self.battery_min = 0
            self.battery_mean = 0
            self.battery_max = 0
