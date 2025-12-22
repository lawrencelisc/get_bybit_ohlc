import schedule
import time
import datetime as dt
from loguru import logger

from core.orchestrator import DataSourceConfig
from core.datacenter import DataCenterSrv


def scheduler(bet_size):
    utc_now = dt.datetime.now(dt.UTC)
    logger.info('Starting algo_seq at (UTC) {}', utc_now.strftime('%Y-%m-%d %H:%M:%S'))

    # 1. Load strategy configuration
    ds = DataSourceConfig()
    ds.create_folder()
    strat_df = ds.load_info_dict()
    logger.info('Loaded #{} rows of strategy configuration', len(strat_df))

    # 2. Build request / data frame
    dcs = DataCenterSrv(strat_df)
    dcs.create_df()
    logger.info('Do data cleaning and update data complete')


if __name__ == '__main__':
    BET_SIZE = {'BTC': 0, 'ETH': 0, 'SOL': 0, 'BNB': 0, 'SUI': 0}

    logger.info('Starting unified scheduler + algo program')

    schedule.every().minute.at(":10").do(scheduler, BET_SIZE)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received; program terminated.')