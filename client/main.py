from client import Client
import logging

def get_config_params():
    return None

def initialize_log(level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def main():
    config_params = get_config_params()
    #initialize_log(config_params['log_level'])

    client = Client(config_params)
    client.start()



if __name__== "__main__":
    main()