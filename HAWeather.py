import logging
import requests
import json


class HAWeather:
    def __init__(self):
        """ Init HA Database Object"""

        logging.info('Creating HAWeather')

        with open('include/credentials.json', 'r') as f:
            credentials = json.load(f)

        self.api_key = credentials['ow-api-key']
        self.zip_code = 80796
        self.weather = ''
        self.email_txt = ''

        self.get_weather()

    def get_weatherframe(self, index_item):
        msg = ''
        try:
            list_item = self.weather['list'][index_item]
            weather_timestamp = list_item['dt_txt']
            weather_description = list_item['weather'][0]['description']
            feels_like = float(
                list_item['main']['feels_like'])
            wind_speed = float(
                list_item['wind']['speed'])
            rain_amount = 0
            if 'rain' in list_item:
                rain_amount = float(
                    list_item['rain']['3h'])

            msg += ('Weather Prevision for: %s \n' % (weather_timestamp))
            msg += ('Weather description: %s \n' % (weather_description))
            msg += ('Temperature Feel: %2.1f °c \n' % (feels_like))
            msg += ('Wind Speed: %2.1f km/h \n' % (wind_speed))
            if rain_amount != 0:
                msg += ('It may rain: %2.1f mm \n' % (rain_amount))

        except Exception as e:  # skipcq: PYL-W0703
            msg = 'Error Reading Weather'
            logging.error(e)

        return msg

    def prepare_email(self):
        """ Prepare E-Mail to be sent """
        email_msg = ''

        index_item = 3
        email_msg = self.get_weatherframe(index_item)
        email_msg += '\n'

        index_item = 6
        email_msg += self.get_weatherframe(index_item)
        email_msg += '\n'

        self.email_txt = email_msg

    def get_weather(self):

        api_default_ulr = 'https://api.openweathermap.org/data/2.5'
        url = api_default_ulr + '/forecast?zip={},{}&units=metric&appid={}'.format(
            self.zip_code, 'DE', self.api_key)
        r = requests.get(url)
        self.weather = r.json()

        self.prepare_email()
