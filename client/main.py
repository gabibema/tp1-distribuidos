from client import Client
import configparser
import logging
import os

def get_config_params():
    config = configparser.ConfigParser()
    config.read('config.ini')

    try:
        config_params = {
            'books_path': os.getenv('BOOKS_PATH', default=config['DEFAULT'].get('BOOKS_PATH')),
            'ratings_path': os.getenv('RATINGS_PATH', default=config['DEFAULT'].get('RATINGS_PATH')),
            'log_level': os.getenv('LOG_LEVEL', default=config['DEFAULT'].get('LOG_LEVEL')),
        }
        print(config_params)
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))
    
    return config_params



def initialize_log(level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def main():
    config_params = get_config_params()
    initialize_log(config_params['log_level'])

    client = Client(config_params)
    client.start()



if __name__== "__main__":
    main()