"""
*********************
Volos Python API
*********************
Documentation at: https://github.com/Francisco-Pina/Business_Analytics
"""

__author__ = 'Francisco Pina'
__copyright__ = '2022 Volos Portfolio Solutions, LLC.. All rights reserved'
__version__ = '2.0'
__interpreter__ = 'Anaconda - Python 3.9.5 64 bit'
__maintainer__ = 'Francisco Pina'
__email__ = 'francisco.pina@volossoftware.com'
__status__ = 'In Progress'

import json
import requests
import pandas as pd


class VolosAPI:

    def __init__(self, volos_api_key):
        self.volos_api_key = volos_api_key
        # by default, the api will be set to strategy
        self.api_endpoint = 'https://api-data-ci-w1.volossoftware.com/ci'

    def set_strategy_api(self):
        self.api_endpoint = 'https://api-data-ci-w1.volossoftware.com/ci'

    def set_index_api(self):
        self.api_endpoint = 'https://api-server-prod-e2.volossoftware.com'

    def get_url(self, endpoint):
        url = self.api_endpoint + endpoint

        return url

    def get_time_series(self, strategy_id):
        self.set_strategy_api()
        endpoint = '/validation/get-validation-df'
        url = self.get_url(endpoint=endpoint)

        obj = {"strategy_id": strategy_id}
        headers = {"x-api-key": self.volos_api_key}

        output = requests.post(url, data=json.dumps(obj), headers=headers).json()

        if list(output.keys())[0] == 'validation_csv_url':
            df = pd.read_csv(output['validation_csv_url'])
        else:
            df = pd.json_normalize(output)

        df = df.loc[:, ['date', 'index_value']]
        df.columns = ['date', 'series_value']
        df.dropna().reset_index(drop=True)
        df['Strategy_id'] = strategy_id
        df = df.loc[:, ['Strategy_id', 'date', 'series_value']]

        return df

    def add_drawdown(self, df):
        list_vals = []
        list_max = []
        for i in df['series_value']:
            list_vals.append(i)
            current_max = max(list_vals)
            list_max.append(current_max)
        df['Peak'] = list_max
        df['Drawdown'] = df['series_value'] - df['Peak']
        df['Drawdown_Percent'] = df['Drawdown'] / df['Peak']
        df = df.loc[:, ['Strategy_id', 'date', 'series_value', 'Drawdown', 'Drawdown_Percent']]

        return df

    def get_metrics(self, strategy_id):
        self.set_strategy_api()
        endpoint = '/time-series/metrics'
        url = self.get_url(endpoint=endpoint)

        obj = {"strategy_id": strategy_id}
        headers = {"x-api-key": self.volos_api_key}

        output = requests.post(url, data=json.dumps(obj), headers=headers).json()

        return output

    def get_validation_data(self, strategy_id):
        self.set_strategy_api()
        endpoint = '/validation/get-validation-df'
        url = self.get_url(endpoint=endpoint)

        obj = {"strategy_id": strategy_id}
        headers = {"x-api-key": self.volos_api_key}

        output = requests.post(url, data=json.dumps(obj), headers=headers).json()

        if list(output.keys())[0] == 'validation_csv_url':
            df = pd.read_csv(output['validation_csv_url'])
        else:
            df = pd.json_normalize(output)

        df.dropna().reset_index(drop=True)
        df['Strategy_id'] = strategy_id

        return df

    def get_annual_returns(self):
        pass

    def save_to_csv(self):
        pass

    def trim_dates(self):
        pass

    def get_info_public_indexes(self):
        self.set_index_api()
        endpoint = '/index/prod/index/all_index_data'
        url = self.get_url(endpoint=endpoint)

        headers = {"content-type": "json"}
        x = requests.post(url, headers=headers)
        index_df = pd.json_normalize(x.json())
        return index_df.loc[:, ['status', 'index_ticker', 'family_name', 'strategy_id',
                                'family_description', 'index_label', 'index_description',
                                'is_public', 'index_id', 'release_date']]


vs = VolosAPI(volos_api_key='4rlmteg2uR2xCV5OtIzJqaTjcxT0edYsL7qPXE20')

strategy_id = 'cf9954a2-0f96-e8a7-58b8-5e198f670f6d'

df = vs.get_time_series(strategy_id)

#df = vs.get_info_public_indexes()

print(df.columns)
print(df.head(5))
