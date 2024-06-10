import json
import logging
from multiprocessing import Manager, Lock

FILE_LOCK = Lock()

class DataSaver:
    def __init__(self, path):
        self.manager = Manager()
        self.shared_last_rows = self.manager.dict()
        self.path = path
        self.__load_from_file()

    def save_message_to_json(self, message):
        uid = message['client_id']

        if uid not in self.shared_last_rows:
            logging.warning(f'Creating new entry for {uid}')
            self.shared_last_rows[uid] = self.manager.dict()

        current_dict = self.shared_last_rows[uid]
        current_dict[message['source']] = message
        self.shared_last_rows[uid] = current_dict

        with FILE_LOCK:
            with open(self.path, 'a') as f:
                json.dump(message, f, indent=4)
                f.write('\n')

    def get_last_row(self, uid):
        return self.shared_last_rows.get(uid, None)
    
    def __load_from_file(self):
        with FILE_LOCK:
            try:
                with open(self.path, 'r') as f:
                    content = f.read()
                    # Dividir el contenido en mensajes JSON individuales
                    messages = content.split('\n}\n{')
                    # Añadir llaves de apertura y cierre adecuadas
                    messages = ['{' + msg + '}' if not msg.startswith('{') else msg + '}' if not msg.endswith('}') else msg for msg in messages]
                    for msg in messages:
                        message = json.loads(msg)
                        uid = message['client_id']
                        if uid not in self.shared_last_rows:
                            self.shared_last_rows[uid] = self.manager.dict()
                        self.shared_last_rows[uid][message['source']] = message
            except FileNotFoundError:
                logging.warning(f'File not found: {self.path}')
            except json.JSONDecodeError:
                logging.error(f'Error while decoding JSON from file: {self.path}')

        normal_dict = {k: dict(v) for k, v in self.shared_last_rows.items()}
        logging.warning(f'Loaded {len(self.shared_last_rows)} entries: {normal_dict}')
