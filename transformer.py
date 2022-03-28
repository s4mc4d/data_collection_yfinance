import pandas as pd
import pathlib


RAW_FOLDER = "./data/raw"
INTERMEDIATE_FOLDER = "./data/intermediate"



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

    # Check if time zone is present and assigns to dt object if any
    if dataframe.index.tz is not None:
        dt = dt.tz_localize(dataframe.index.tz)
    
    # Reindex dataframe
    newdf = dataframe.reindex(dt)

    if new_tz is not None:
        newdf = newdf.tz_convert(new_tz)        
    
    return newdf


if __name__=="__main__":
    for item in pathlib.Path(RAW_FOLDER).glob("*.pkl"):
        print("")
        print(item)
        df = pd.read_pickle(item)
        print(df.shape)
        df = regulate_datetime_interval(df)
        print(df.shape)
        df.to_pickle(pathlib.Path(INTERMEDIATE_FOLDER) / str(item.name))

        