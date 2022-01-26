"""
*********************
Volos Python API
*********************
Documentation at: https://github.com/Francisco-Pina/Volos-API-2
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

    def __init__(self, volos_api_key=None):
        # create function to test API key
        self.volos_api_key = volos_api_key
        # by default, the api will be set to strategy
        self.api_endpoint = 'https://api-data-ci-w1.volossoftware.com/ci'

    def set_strategy_api(self):
        self.api_endpoint = 'https://api-data-ci-w1.volossoftware.com/ci'

    def set_index_api(self):
        self.api_endpoint = 'https://api-server-prod-e2.volossoftware.com'

    def get_url(self, endpoint):
        return self.api_endpoint + endpoint

    def try_index_first(self, strategy_id):
        index_df = self.get_info_public_indexes()
        index_strat_id = None

        if strategy_id in index_df['strategy_id'].tolist() or strategy_id in index_df['index_ticker'].tolist():
            try:
                index_id = index_df.loc[index_df['strategy_id'] == strategy_id, 'index_id'].item()
                index_strat_id = index_df.loc[index_df['strategy_id'] == strategy_id, 'strategy_id'].item()

            except:
                index_id = index_df.loc[index_df['index_ticker'] == strategy_id, 'index_id'].item()
                index_strat_id = index_df.loc[index_df['index_ticker'] == strategy_id, 'strategy_id'].item()
            return True, index_id, index_strat_id

        else:
            return False, strategy_id, index_strat_id

    def get_time_series_index(self, index_id):
        self.set_index_api()
        endpoint = '/index/prod/index/get_time_series'
        url = self.get_url(endpoint=endpoint)

        obj = {"index_id": index_id}
        headers = {"content-type": "json"}
        x = requests.post(url, data=json.dumps(obj), headers=headers)

        index_df = pd.json_normalize(x.json())
        index_df['Strategy_id'] = index_id

        return index_df

    def get_time_series(self, strategy_id):

        test_true_false, test_index, _ = self.try_index_first(strategy_id)
        df = None

        if not test_true_false:
            try:
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
                df = df.loc[:, ['date', 'series_value', 'Strategy_id']]

            except ValueError as e:
                return {'Error': str(e)}

        elif test_true_false:
            ####
            df = self.get_time_series_index(test_index)

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
        df = df.loc[:, ['date', 'series_value', 'Strategy_id', 'Drawdown', 'Drawdown_Percent']]

        return df

    def get_metrics(self, strategy_id):

        test_true_false, test_index, index_strat_id = self.try_index_first(strategy_id)
        df = None

        if not test_true_false:
            self.set_strategy_api()
            endpoint = '/time-series/metrics'
            url = self.get_url(endpoint=endpoint)

            obj = {"strategy_id": strategy_id}
            headers = {"x-api-key": self.volos_api_key}

            x = requests.post(url, data=json.dumps(obj), headers=headers)
            output = 'date' + x.text
            data = [y.split(",") for y in output.split("\n")]
            df = pd.DataFrame(data[1:], columns=data[0]).dropna().tail(1).reset_index(drop=True)
            df.insert(1, 'Strategy_id', index_strat_id)

        elif test_true_false:
            self.set_index_api()
            endpoint = '/index_summary/prod/index_summary/metrics'
            url = self.get_url(endpoint=endpoint)

            print(index_strat_id)

            obj = {"strategy_id": index_strat_id}
            headers = {"content-type": "text/plain"}
            x = requests.post(url, data=json.dumps(obj), headers=headers)

            df = pd.json_normalize(x.json()['metrics'])
            df.insert(0, 'date', pd.Timestamp.today().strftime('%Y-%m-%d'))
            df.insert(1, 'Strategy_id', strategy_id)

            ######## Columns to Percent ########
            cols_to_percentage = ['Annual_Returns', 'Downside_Risk', 'Sortino_Ratio',
                                  'Annual_Volatility', 'Max_Drawdown', 'Tail_Ratio', 'CAGR',
                                  'Profit_Prob', 'Calmar_Ratio', 'Cum_Returns_Final']
            df.loc[:, cols_to_percentage] = df.loc[:, cols_to_percentage].div(100, axis=0)

        return df

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

    def get_annual_returns(self, df):

        returns_df = df.iloc[df.reset_index().groupby(df.date.to_period('Y'))['date'].idxmax()]

        return None

    def save_to_csv(self, df, name):
        """Saves file to current folder"""
        return df.to_csv(name + '.csv', index=False)

    def trim_dates(self, df, start_date, end_date):
        """Format: YYYY-MM-DD
        Function includes start and end date"""
        return df[(df['date'] >= start_date) & (df['date'] <= end_date)]

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


if __name__ == "__main__":
    temporary_file = open("../volos_api_key.txt", "rt")
    volos_api_key = temporary_file.read()
    temporary_file.close()

    vs = VolosAPI(volos_api_key=volos_api_key)

    strategy_id = 'VOCCGLD100'

    df = vs.get_time_series(strategy_id)

    df_metrics = vs.get_metrics(strategy_id)

    # print(df)
    print(df)
    print(df_metrics)
