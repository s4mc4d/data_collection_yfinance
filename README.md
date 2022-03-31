# Projet Final JEDHA

Repo pour la phase exploratoire et démonstration de l'API yfinance chez Jedha.


## Description

- `extractor.py` : launches extraction of stocks history from yfinance and stores to RAW_FOLDER under the form of pkl files

    > python3 extractor.py

- `load_config.py` : reads config.yml file and delivers all configuration constants. No launching required.

- `transformer.py` : reads pkl files from RAW_FOLDER and cleans data. Stores results to INTERMEDIATE_FOLDER. Cleaning includes regularization of DateTimeIndex and timezone handling. Classical launch is:
    ```shell
    python3 transformer.py 
    ```

- `loader.py` : takes provided files and uploads them to AWS S3 /datalake folder. The Bucket information is given in config.yml. TYpe the following for further details.
    ```shell
    python3 loader.py --help
    ```

    This script needs the following AWS credentials to be set in the environment variables:

    ```shell
    export AWS_S3_BUCKET_NAME="example_bucket";
    export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE";
    export AWS_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    ```

## TODO

- Create entrypoint program for the whole pipeline
- Manage full folders in loader program. 
- Factorize code in loader and transformer as OOP classes