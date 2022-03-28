"""This program extracts list of stocks history from yfinance 
    and saves it to pickle file.
    Here are operations that are performed :
    1. Get list of stocks from a mapping dictionary
    2. Get history of each stock from yfinance
    3. Regularizes the Datetime Index
    4. Changes the timezone to UTC
    5. Save history to pickle file
"""

import pathlib
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import plotly.express as px
import pytz
from dateutil.parser import isoparse

RAW_FOLDER = "./data/raw"
# YF_PERIOD = "max"  #1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (optional, default is '1mo')
YF_INTERVAL = "1m" # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (optional, default is '1d')
USE_CACHED = True
END_DAY = "2022-03-23"
NB_DAYS = 7  # number of days to extract


def download_history(ticker, start_date, end_date, interval, use_cached):
    """Downloads history of a stock from yfinance

    Parameters
    ----------
    ticker : str
        ticker of the stock
    start_date : str
        start date of the history
    end_date : str
        end date of the history
    interval : str
        interval of the history
    use_cached : bool
        if True, the history is downloaded from yfinance cache, if False, it is downloaded from yfinance

    Returns
    -------
    pd.DataFrame
        history of the stocks
    """
    if use_cached:
        df = yf.download(ticker, start=start_date, end=end_date, interval=interval)
    else:
        df = yf.download(ticker, start=start_date, end=end_date, interval=interval, group_by="ticker")
    return df


if __name__=="__main__":
    symbols_mapping = {"Tesla":"TSLA",
                        "Pfizer":"PFE",
                        "Hermès":"RMS.PA",
                        "Toyota":"TM",
                        "Tencent":"TCEHY",
                        "Rio Tinto":"RIO",
                        "Alphabet":"GOOGL",
                        "BMW":"BMWYY",
                        "Dassault Systèmes":"DSY.PA",
                        "EDF":"EDF.PA",
                        "BTC-USD":"BTC-USD"}

    data = {}
    for key,val in symbols_mapping.items():
        value = val
        value_formatted = value.replace(".","p")
        key_formatted = key.replace(" ","_")
        print(value_formatted)

        RAW_FOLDER = pathlib.Path(RAW_FOLDER)
        storage_path = RAW_FOLDER / f"{key}_{value_formatted}_{NB_DAYS}d_{YF_INTERVAL}.pkl"

        if USE_CACHED and storage_path.exists(): # if USE_CACHED, then we read from pickle file.
            tempdf = pd.read_pickle(storage_path)
            print(key + " USed cached data")
        
        else:
            
            end_datetime = datetime.datetime.strptime(END_DAY, "%Y-%m-%d")
            end_datetime = end_datetime.replace(tzinfo=pytz.UTC)
            end_datetime = end_datetime.replace(hour=23,minute=59,second=59)

            start_datetime = end_datetime-datetime.timedelta(days=NB_DAYS)

            print(f"Duration of requested extraction : {end_datetime-start_datetime}")

            tempdf = yf.download(value,
                                start=start_datetime,
                                end=end_datetime,
                                interval=YF_INTERVAL)
            if len(tempdf):
                print(key,"Shape : ",tempdf.shape)
                
                tempdf.to_pickle(storage_path)
                data[key] = tempdf.copy(deep=True)
                print(key + " Downloaded")

        
    