# Volos-API-2
Second version of volos API using the Data API 

## Setup

Using git, move to your local repository and clone the project
```
cd /your/local/repo/here
git clone https://github.com/Francisco-Pina/Volos-API-2.git
```


## Getting Started

The volosAPI is a tool that allows users to access data from the Volos software platform. The API allows users to access data on strategies, trades, positions, and more.

To start, you can initiate the class with your API key and the strategy ID given by Volos 

```python
import json
import requests
import pandas as pd
import io
import os
import datetime
import sys

sys.path.insert(0, r'C:\Users\your\repo\here\Volos-API-2') # change to your local path

from volosAPI import volosAPI

strategy_id = 'your_strategy_id_here'
vs = volosAPI(api_key="your_api_key_here")
vs.switch_stage_ci()
```

## Use Cases

```python
import matplotlib.pyplot as plt

# returns a DataFrame with the daily returns of a given strategy.
total_returns = vs.get_strategy_total_returns(strategy_id)
total_returns.plot(x='date')

# returns a DataFrame with the daily returns of a given list of strategies.
multiple_returns = vs.get_strategy_list_total_returns([strategy_id,strategy_id_2])
multiple_returns.plot(x='date')

# returns a DataFrame with the metrics of a given strategy.
strategy_metrics = vs.get_strategy_metrics(strategy_id)
sliced_metrics = strategy_metrics.head(14)
fig, ax = plt.subplots(figsize=(25, 5))

# hide axes
fig.patch.set_visible(False)

ax.axis('off')
ax.axis('tight')

ax.table(cellText=sliced_metrics.values, colLabels=sliced_metrics.columns, loc='center')

plt.title('Metrics')

fig.tight_layout()

plt.show()

# returns a DataFrame with the trade logs of a given strategy.
trade_logs = vs.get_strategy_trade_logs(strategy_id)
trade_logs.loc[trade_logs['transaction_date'] == '2022-05-18']

# returns an IO with the positions of a given strategy on a given date.
trade_positions = vs.get_strategy_positions(strategy_id, on_date='2022-05-26')
trade_positions_df = pd.read_csv(trade_positions, index_col=[0])

# returns a json with the metadata of a given list of strategies.
multiple_meta_data = vs.get_strategy_list_meta_data([strategy_id,strategy_id_2])
print(multiple_meta_data)

# returns a DataFrame with the positions of a given strategy over time.
vs.get_timeseries_positions(strategy_id)

# returns a DataFrame with the values of a given strategy's positions over time.
vs.get_timeseries_positions_values(strategy_id)

# returns a DataFrame with the metadata of a given strategy's positions.
strategy_positions_metadata = vs.get_strategy_positions_meta_data(strategy_id)
strategy_positions_metadata.sort_values(by=['expiration'])

# saves the positions of a given strategy to an Excel sheet.
vs.save_positions_to_excel(strategy_id)

# returns a DataFrame with information on public indexes.
volos_indexes = vs.get_info_public_indexes()
volos_indexes.loc[volos_indexes['index_ticker'] == 'TOPSGB']

```