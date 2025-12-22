import yaml
import os
import gc
import pandas as pd

from pathlib import Path
from loguru import logger
from decimal import Decimal
from datetime import date, timedelta, datetime, tzinfo

class DataSourceConfig:


    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)


    def __init__(self):
        self.strat_df = None


    # Creat data folder and sub-folder at initial
    def create_folder(self):
        base_path = Path(__file__).parent.parent.parent / 'share_algoB'
        folders = ['bybit_data']
        for folder_name in folders:
            folder_path = base_path / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)
        gc.collect


    # Load su_table becoming strat_df
    def load_info_dict(self):
        try:
            project_root = os.path.dirname(os.path.dirname(__file__))
            csv_path = os.path.join(project_root, 'config', 'su_table.csv')
            self.strat_df = pd.read_csv(csv_path)
            strat_df = self.strat_df.copy()
        except FileNotFoundError:
            logger.error('su_table.csv not found.')
        gc.collect
        return strat_df


    # Load bybit API keys from config.yaml
    def load_bybit_api_config(self, symbol: str):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                bybit_sub = 'algo_1m_' + symbol.lower()
                bybit_sub_api = config.get(bybit_sub, {})
                sub_api = symbol + '_1M_API_KEY'
                sub_secret = symbol + '_1M_SECRET_KEY'
                required_keys = [sub_api, sub_secret]
                for key in required_keys:
                    if key not in bybit_sub_api or not bybit_sub_api[key]:
                        raise Exception(f'Missing or empty {key} in config.yaml')
                return bybit_sub_api
        except FileNotFoundError:
            raise Exception('config.yaml not found.')
        except Exception as e:
            raise Exception(f'Error loading config.yaml: {e}')
        gc.collect