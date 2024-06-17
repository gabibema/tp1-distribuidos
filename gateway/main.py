from configparser import ConfigParser
from gateway import Gateway
import logging
import os 

def get_config_params():
    config = ConfigParser()
    config.read('config.ini')

    try:
        config_params = {
            'result_queues': config['DEFAULT'].get('RESULT_QUEUES').split(','),
            'port': int(os.getenv('PORT', default=config['DEFAULT'].get('PORT'))),
            'log_level': os.getenv('LOG_LEVEL', default=config['DEFAULT'].get('LOG_LEVEL')),
            'records_path': os.getenv('RECORDS_PATH', default=config['DEFAULT'].get('RECORDS_PATH')),
            'results_path': os.getenv('RESULTS_PATH', default=config['DEFAULT'].get('RESULTS_PATH')),
        }
        print(config_params)
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))
    
    return config_params

def initialize_log(level=logging.WARNING):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def initialize_files(records_path, results_path):
    if not os.path.exists(records_path):
        with open(records_path, 'w') as _:
            pass

    if not os.path.exists(results_path):
        with open(results_path, 'w') as _:
            pass

def main():
    config_params = get_config_params()
    initialize_files(config_params['records_path'], config_params['results_path'])
    initialize_log(config_params['log_level'])

    gateway = Gateway(config_params)
    gateway.start()

if __name__== "__main__":
    main()
