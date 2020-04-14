import logging
import sqlite3
import pandas as pd
import shutil
import os


class HADatabase:
    def __init__(self,path=''):
        logging.info('Creating HADatabase - Just Assigning Path')
        if (path != ''): self.path = path
        else: self.path = ''

    def read_database(self):  
        try:  # First copy the database file to the working directory
            logging.info('Reading Database -  Copying DB to Working Dir')
            self.copy_db_to_working_dir()
            try:  # Then connect to the file
                self.con = sqlite3.connect(self.db_working_dir)
                logging.info('Connecting Succeded!')
                try:  # Then read the Database File and save it in a Pandas Dataframe
                    logging.info('Reading Database - Saving Data into Dataframe')
                    self.df_db = pd.read_sql_query('SELECT * FROM states;',self.con)
                    self.con.close()
                    try:  # Then save the Dataframe into the csv to create statistics
                        logging.info('Reading Database - Creating Backup in CSV')
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
                logging.error('%s %s',e,self.path)
                self.con = 0 
        except Exception as e:  # skipcq: PYL-W0703
            logging.error('Error Copying DB to Working Dir')
            logging.error('%s %s',e,self.path)

    def copy_db_to_working_dir(self):
        self.db_working_dir = 'data/home-assistant.db'
        try:
            shutil.copyfile(self.path,self.db_working_dir)
        except Exception as e:  # skipcq: PYL-W0703
            logging.error(e)

    def save_to_csv_db(self):
        self.csv_working_dir = 'data/home-assistant.csv'
        self.df_db.to_csv(self.csv_working_dir)
        # TO DO read previous CSV and append new information
        # TO DO Clean Database

    def prepare_email(self):
        self.email_txt = 'Hola'

        self.df_csv = pd.read_csv(self.csv_working_dir)

        self.calculate_battery_values()
        self.email_txt = self.email_txt + str(self.battery_min)

    def calculate_battery_values(self):
        try:
            x = self.df_csv[(self.df_csv['entity_id'] =='sensor.battery_status_garden')].replace('unknown',method='bfill')['state'].astype(float)
            self.battery_min = x.min()
            self.battery_mean = x.mean()
            self.battery_max = x.max()
        except Exception as e:  # skipcq: PYL-W0703
            logging.error(e)
            self.battery_min = 0
            self.battery_mean = 0
            self.battery_max = 0