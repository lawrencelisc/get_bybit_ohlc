import os
import gc
import ast
import sys
import json
import pytz
import requests
import pandas as pd
import warnings
import ccxt

from io import StringIO
from loguru import logger
from pathlib import Path
from datetime import datetime, timezone

from core.orchestrator import DataSourceConfig
from ccxt.base.exchange import Exchange


class DataCenterSrv:
    warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
    dict_output_key = 'o'
    data_folder_bybit = Path(__file__).parent.parent.parent / 'share_algoB' / 'bybit_data'


    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df


    def get_exchange_trade(self, symbol: str):
        market_symbol = f'{symbol}/USDT:USDT'
        try:
            bybit_cfg = DataSourceConfig()
            bybit_api = bybit_cfg.load_bybit_api_config(symbol)
            self.bybit = ccxt.bybit()
            # self.bybit = ccxt.bybit({
            #     'apiKey': bybit_api[symbol + '_1M_API_KEY'],
            #     'secret': bybit_api[symbol + '_1M_SECRET_KEY'],
            #     'enableRateLimit': True,
            #     'options': {'default': 'swap'},
            # })
            self.markets = self.bybit.load_markets()
        except Exception as e:
            logger.exception("Failed to load exchange info for %s: %s", symbol, e)
            raise
        try:
            market = self.markets[market_symbol]
            return market
        except KeyError:
            logger.error("No matching market for %s", symbol)
            return None


    def get_bybite_data(self, unix_since: int, symbol: str, res: str):
        df_bybit = []
        bybit_get_since = datetime.fromtimestamp(unix_since,
                                                 tz=timezone.utc)

        unix_since_ms = int(unix_since * 1000)
        symbol_usdt = symbol + 'USDT'
        self.get_exchange_trade(symbol)
        bybit_get_data = self.bybit.fetchOHLCV(symbol_usdt, res, since=unix_since_ms)

        df = pd.DataFrame(bybit_get_data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['datetime'], unit='ms', utc=True)
        cols = ['datetime', 'date', 'close', 'high', 'low', 'open', 'volume']
        df = df[cols]

        df['date'] = pd.to_datetime(df['datetime'], unit='ms', utc=True)
        df = df.set_index('date')
        df_bybit = df.copy()
        df_bybit = df_bybit.rename(columns={'close': 'c', 'high': 'h', 'low': 'l', 'open': 'o', 'volume': 'v',})
        return df_bybit


    def create_df(self):
        # Validations
        if self.strat_df is None or self.strat_df.empty:
            logger.error('strat_df is empty or None. Provide a non-empty DataFrame.')
            return

        required_cols = {'name', 'symbol'}
        missing = required_cols - set(self.strat_df.columns)
        if missing:
            logger.error(f'strat_df missing required columns: {missing}')
            return

        # Time window
        res = '1m'
        unix_diff = 200 * 60                   # max: 200 prev data from bybit
        until_iso = datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_until = datetime.strptime(until_iso, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        unix_until = int(dt_until.timestamp())
        unix_since = unix_until - unix_diff

        dict_output = self.dict_output_key
        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})

        def data_cleaning_dict(x):
            # Normalizes cell to dict or None
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            if isinstance(x, dict):
                return x
            strip_data = str(x).strip()
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(strip_data)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    continue
            return None


        # load data from bybit datasource for each strategy
        for _, row in self.strat_df.iterrows():
            combined_df = pd.DataFrame()

            name: str = str(row['name'])
            symbol: str = str(row['symbol'])
            filename: str = f'{name}_{symbol}.parquet'
            file_path = self.data_folder_bybit / filename

            # Determine fetch_since based on existing file
            if not file_path.exists():
                # Fresh to full download
                df_bybit = self.get_bybite_data(unix_since, symbol, res)
                df_bybit = df_bybit.reset_index()
                existing_df = df_bybit.copy()
                try:
                    df_bybit.to_parquet(file_path)
                    logger.info(f'Created file with {len(df_bybit)} rows for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save parquet for {symbol}: {e}')
            else:
                # Update existing file
                try:
                    existing_df = pd.read_parquet(file_path)
                except Exception as e:
                    logger.error(f'Failed to read existing parquet {filename}: {e}')
                    existing_df = pd.DataFrame()

                # if find existing_df exists but empty, treat as fresh
                if existing_df.empty:
                    df_bybit = self.get_bybite_data(unix_since, symbol, res)
                    df_bybit = df_bybit.reset_index()
                    existing_df = df_bybit.copy()
                    try:
                        df_bybit.to_parquet(file_path)
                        logger.info(f'Created file with {len(df_bybit)} rows for {symbol}: {filename}')
                    except Exception as e:
                        logger.error(f'Failed to save parquet for {symbol}: {e}')

            existing_df = existing_df.sort_index()
            latest_ts = existing_df['date'].iloc[-1]
            unix_latest_ts = int(pd.to_datetime(latest_ts).timestamp())
            unix_diff = int(unix_until - unix_latest_ts)

            latest_ts = pd.to_datetime(unix_latest_ts, unit='s', utc=True)
            until = pd.to_datetime(unix_until, unit='s', utc=True)

            if (unix_diff > 60):
                df_bybit_new = self.get_bybite_data(unix_latest_ts, symbol, res)
                df_bybit_new = df_bybit_new.reset_index()

                combine_bybit_df = pd.concat(
                    [existing_df.iloc[:-1].dropna(how='all', axis=1),
                     df_bybit_new.dropna(how='all', axis=1)],
                    axis=0
                ).reset_index(drop=True)

                try:
                    combine_bybit_df.to_parquet(file_path)
                    logger.info(f'Update file with {len(df_bybit_new) - 1} row(s) for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save parquet for {symbol}: {e}')

        gc.collect
        return


    def create_df_csv(self):
        # Validations
        if self.strat_df is None or self.strat_df.empty:
            logger.error('strat_df is empty or None. Provide a non-empty DataFrame.')
            return

        required_cols = {'name', 'symbol'}
        missing = required_cols - set(self.strat_df.columns)
        if missing:
            logger.error(f'strat_df missing required columns: {missing}')
            return

        # Time window
        res = '1m'
        unix_diff = 200 * 60                   # max: 200 prev data from bybit
        until_iso = datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_until = datetime.strptime(until_iso, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        unix_until = int(dt_until.timestamp())
        unix_since = unix_until - unix_diff

        dict_output = self.dict_output_key
        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})

        def data_cleaning_dict(x):
            # Normalizes cell to dict or None
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            if isinstance(x, dict):
                return x
            strip_data = str(x).strip()
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(strip_data)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    continue
            return None


        # load data from bybit datasource for each strategy
        for _, row in self.strat_df.iterrows():
            combined_df = pd.DataFrame()

            name: str = str(row['name'])
            symbol: str = str(row['symbol'])
            filename: str = f'{name}_{symbol}.csv'
            file_path = self.data_folder_bybit / filename

            # Determine fetch_since based on existing file
            if not file_path.exists():
                # Fresh to full download
                df_bybit = self.get_bybite_data(unix_since, symbol, res)
                existing_df = df_bybit.copy()
                try:
                    df_bybit.to_csv(file_path)
                    logger.info(f'Created file with {len(df_bybit)} rows for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save csv for {symbol}: {e}')
            else:
                # Update existing file
                try:
                    existing_df = pd.read_csv(file_path, index_col='date')
                except Exception as e:
                    logger.error(f'Failed to read existing csv {filename}: {e}')
                    existing_df = pd.DataFrame()

                # if find existing_df exists but empty, treat as fresh
                if existing_df.empty:
                    df_bybit = self.get_bybite_data(unix_since, symbol, res)
                    existing_df = df_bybit.copy()
                    try:
                        df_bybit.to_csv(file_path)
                        logger.info(f'Created file with {len(df_bybit)} rows for {symbol}: {filename}')
                    except Exception as e:
                        logger.error(f'Failed to save csv for {symbol}: {e}')

            existing_df = existing_df.sort_index()
            latest_ts = existing_df.index[-1]
            unix_latest_ts = int(pd.to_datetime(latest_ts).timestamp())
            unix_diff = int(unix_until - unix_latest_ts)

            latest_ts = pd.to_datetime(unix_latest_ts, unit='s', utc=True)
            until = pd.to_datetime(unix_until, unit='s', utc=True)

            if (unix_diff > 60):
                df_bybit_new = self.get_bybite_data(unix_latest_ts, symbol, res)

                combine_bybit_df = pd.concat(
                    [existing_df.iloc[:-1].dropna(how='all', axis=1),
                     df_bybit_new.dropna(how='all', axis=1)],
                    axis=0
                )

                try:
                    combine_bybit_df.to_csv(file_path)
                    logger.info(f'Update file with {len(df_bybit_new) - 1} row(s) for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save parquet for {symbol}: {e}')

        gc.collect
        return