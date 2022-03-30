# Projet Final JEDHA

Repo pour la phase exploratoire et démonstration de l'API yfinance chez Jedha.


## Description

- `extractor.py` : launches extraction of stocks history from yfinance and stores to RAW_FOLDER under the form of pkl files

    > python3 extractor.py

- `load_config.py` : reads config.yml file and delivers all configuration constants. No launching required.
- `transformer.py` : reads pkl files and cleans data. Stores results to INTERMEDIATE_FOLDER. Cleaning includes regularization of DateTimeIndex
  
    > python3 transformer.py -s pkl_file_path -t clean_pkl_file_path
