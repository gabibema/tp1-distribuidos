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
            'reviews_path': os.getenv('REVIEWS_PATH', default=config['DEFAULT'].get('REVIEWS_PATH')),
            'output_dir': os.getenv('OUTPUT_DIR', default=config['DEFAULT'].get('OUTPUT_DIR')),
            'batch_amount': int(os.getenv('BATCH_AMOUNT', default=config['DEFAULT'].get('BATCH_AMOUNT'))),
            'port': int(os.getenv('PORT', default=config['DEFAULT'].get('PORT'))),
            'log_level': os.getenv('LOG_LEVEL', default=config['DEFAULT'].get('LOG_LEVEL')),
            'client_id': os.getenv('CLIENT_ID'),
        }
        print(config_params)
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))
    
    return config_params



def initialize_log(level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def initialize_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file in os.listdir(output_dir):
        os.remove(os.path.join(output_dir, file))
        
def main():
    config_params = get_config_params()
    initialize_log(config_params['log_level'])
    initialize_dir(config_params['output_dir'])

    client_timeout = True
    while client_timeout:
        client = Client(config_params)
        client_timeout = client.start()




if __name__== "__main__":
    main()

