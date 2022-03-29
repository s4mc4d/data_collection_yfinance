"""From simple launch in CLI, cleans data from RAW_FOLDER and stores it in CLEAN_FOLDER
Cleaning includes : 
    1. Regularizes the Datetime Index
    2. Changes the timezone to UTC
"""

import logging
import pathlib

import pandas as pd
import pytz

from load_config import *

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03dZ %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
logging.getLogger().setLevel(logging.INFO)

def get_timezone():
    """Detects timezone from config file, otherwise uses UTC

    Returns
    -------
    pytz.timezone
        New time zone for dataframe
    """
    try:
        tz = pytz.timezone(TIMEZONE_NEW_NAME)
        logging.getLogger(__name__).debug("Converson of timezone succeeded.")
    except pytz.exceptions.UnknownTimeZoneError:
        tz = pytz.utc
        logging.getLogger(__name__).debug(f"{TIMEZONE_NEW_NAME} is not a valid timezone. UTC is used instead")
    except ValueError:
        tz = pytz.utc
        logging.getLogger(__name__).debug(f"TIMEZONE_NEW_NAME is not defined in config.yml. UTC is used instead")
    return tz


def regulate_datetime_interval(dataframe,freq="1min",new_tz=None):
    """Reindexes datetime indexed dataframe to regular intervals and adapts timezone

    Parameters
    ----------
    dataframe : pd.DataFrame
        input dataframe with DateTimeIndex as index 
    freq : str, optional
        regular interval according to https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases, by default "1min"
    new_tz : pytz.timezone, optional
        new_tz: timezone of the new datetime index. 
                If None, the timezone of the original datetime index is used
                by default None

    Returns
    -------
    pd.DataFrame
        Output Dataframe with regular intervals in index and adjusted timezone
    """
    # returns: pandas dataframe

    # Get the first and last datetime
    first = dataframe.index.min()
    last = dataframe.index.max()
    
    # Create regular datetimes
    dt = pd.date_range(first, last, freq=freq,tz=None)
    logging.getLogger(__name__).debug(f"New date_range : {len(dt)}")  
    
    # Check if time zone is present and assigns to dt object if any
    if (dataframe.index.tz is not None) and (dataframe.index.tzinfo.zone!="UTC"):
        dt = dt.tz_localize(dataframe.index.tz)
    
    # Reindex dataframe
    newdf = dataframe.reindex(dt)

    if new_tz is not None:
        newdf = newdf.tz_convert(new_tz)        
    
    return newdf


if __name__=="__main__":
    for item in pathlib.Path(RAW_FOLDER).glob("*.pkl"):
        logging.getLogger(__name__).info("Reading file...")
        logging.getLogger(__name__).info(f"{item}")

        df = pd.read_pickle(item)
        
        logging.getLogger(__name__).debug(f"{df.shape}")
        df = regulate_datetime_interval(df,freq="1min")
        logging.getLogger(__name__).debug(f"{df.shape}")
        if len(df):
            df.to_pickle(pathlib.Path(INTERMEDIATE_FOLDER) / ("clean_"+str(item.name)))
            logging.getLogger(__name__).debug(f"Dataframe saved in {INTERMEDIATE_FOLDER}")
        else:
            logging.getLogger(__name__).warning(f"Dataframe is empty")        