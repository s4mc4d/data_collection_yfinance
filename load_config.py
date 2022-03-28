import yaml

# Read YAML file
with open("config.yml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
    RAW_FOLDER = data_loaded.get(data_loaded["YF_INTERVAL"],"./data/raw")
    INTERMEDIATE_FOLDER : data_loaded.get(data_loaded["YF_INTERVAL"],"./data/intermediate")
    YF_INTERVAL = data_loaded.get(data_loaded["YF_INTERVAL"],"1m") # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (optional, default is '1d')
    USE_CACHED = data_loaded["USE_CACHED"]
    END_DAY = data_loaded["END_DAY"]
    SYMBOLS_MAPPING = data_loaded["symbols"]
    NB_DAYS = data_loaded.get(data_loaded["NB_DAYS"],7)  # number of days to extract up to END_DAY
    PARALLEL_DOWNLOAD = data_loaded.get(data_loaded["PARALLEL_DOWNLOAD"],True)
