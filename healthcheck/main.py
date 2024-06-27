from os import getenv
from healthchecker import HealthChecker

def get_config_params():
    try:
        config_params = {
            'id': int(getenv('ID')),
            'hostname': getenv('HOSTNAME'),
        }
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))
    
    return config_params

def main():
    config_params = get_config_params()
    healtcheck = HealthChecker(config_params)
    healtcheck.start()

if __name__== "__main__":
    main()