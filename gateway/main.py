from configparser import ConfigParser
from gateway import Gateway
import logging
import os 

def get_config_params():
    config = ConfigParser()
    config.read('config.ini')

    try:
        config_params = {
            'books_exchange': os.getenv('BOOKS_EXCHANGE', default=config['DEFAULT'].get('BOOKS_EXCHANGE')),
            'ratings_exchange': os.getenv('RATINGS_EXCHANGE', default=config['DEFAULT'].get('RATINGS_EXCHANGE')),
            'port': int(os.getenv('PORT', default=config['DEFAULT'].get('PORT'))),
            'log_level': os.getenv('LOG_LEVEL', default=config['DEFAULT'].get('LOG_LEVEL')),
        }
        print(config_params)
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))
    
    return config_params

def initialize_log(level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def main():
    config_params = get_config_params()
    initialize_log(config_params['log_level'])

    gateway = Gateway(config_params)
    gateway.start()

if __name__== "__main__":
    main()
