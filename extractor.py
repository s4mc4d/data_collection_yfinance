"""This program extracts list of stocks history from API 
    and saves it to pickle files.
    Here are operations that are performed :
    1. Get list of stocks from a mapping dictionary
    2. Get history of each stock from yfinance
    3. Save history to pickle file
"""

import datetime
import logging
import pathlib

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import pytz
import yfinance as yf
from dateutil.parser import isoparse

# Loading all constants
from load_config import *

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03dZ %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
logging.getLogger(__name__).setLevel(logging.DEBUG)


def _download_history_to_pkl(symbols_list,
                                start_date,
                                end_date,
                                interval="1min",
                                use_cached=True,
                                filepath=None):
    """Downloads history of single or multiple stocks from yfinance and saves it to pickle file.

    Parameters
    ----------
    symbols_list : str
        Symbols list of strings of the stocks as expected by yfinance.
    start_date : str or datetime.datetime
        start date of the history.
    end_date : str or datetime.datetime
        end date of the history.
    interval : str, optional
        granularity of request, by default "1min"
        Valid intervals are 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    use_cached : bool, optional
        If True then reads from filepath, by default True
    filepath : str or pathlib.Path, optional
        Location of data, by default None. If None then returns only dataframe.

    Returns
    -------
    pandas.DataFrame
        Output dataframe with history of the stock (Open, Low, High, Close, Adj Close, Volume)
    """

    logging_string = "/".join(symbols_list)

    if use_cached and pathlib.Path(filepath).exists():
        tempdf = pd.read_pickle(filepath)
        logging.getLogger(__name__).debug(f"{logging_string} Used cached data")
    else:
        tempdf = yf.download(symbols_list,
                            start=start_date,
                            end=end_date,
                            interval=interval,
                            # group_by="columns"
                            )
        if not len(tempdf):
            logging.getLogger(__name__).error(f"{logging_string} No data found")
        else:
            logging.getLogger(__name__).debug(f"{logging_string} Shape : {tempdf.shape}")
            tempdf.to_pickle(filepath)
            logging.getLogger(__name__).debug(f"{logging_string} Downloaded")
    return tempdf


def download_stocks(symbols_dict, 
                    # interval : str = "1min",
                    parallel : bool = True,
                    target_folder=None):
    
    end_datetime = datetime.datetime.strptime(END_DAY, "%Y-%m-%d")
    # end_datetime = end_datetime.replace(tzinfo=pytz.UTC)
    end_datetime = end_datetime.replace(hour=23,minute=59,second=59)
    start_datetime = end_datetime-datetime.timedelta(days=NB_DAYS)
    logging.getLogger(__name__).debug(f"Understood {start_datetime} to {end_datetime}")

    target_path = pathlib.Path(target_folder)

    if not parallel:
        for key,val in symbols_dict.items():
            value = val
            value_formatted = value.replace(".","p")
            print(value_formatted)

            storage_path = target_path / f"{key}_{value_formatted}_{NB_DAYS}d_{YF_INTERVAL}.pkl"

            _download_history_to_pkl(value,
                                        start_datetime,
                                        end_datetime,
                                        interval=YF_INTERVAL,
                                        use_cached=USE_CACHED,
                                        filepath=storage_path)

            logging.getLogger(__name__).info(f"Duration of requested extraction : {end_datetime-start_datetime}")
    else:
        # Parallel download
        storage_path = target_path / f"all_tickers_{YF_INTERVAL}.pkl"
        
        _download_history_to_pkl(symbols_list=list(symbols_dict.values()),
                                start_date=start_datetime,
                                end_date=end_datetime,
                                interval=YF_INTERVAL,
                                use_cached=False,
                                filepath=storage_path)
    
    return 0


if __name__=="__main__":
    # Loads all raw symbols history and stores it to RAW_FOLDER
    _ = download_stocks(symbols_dict=SYMBOLS_MAPPING,
                    parallel = True,
                    target_folder=RAW_FOLDER)

    

        
    