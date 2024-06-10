import json
import logging
from multiprocessing import Manager, Lock

FILE_LOCK = Lock()

class DataSaver:
    def __init__(self, path):
        self.manager = Manager()
        self.shared_last_rows = self.manager.dict()
        self.path = path

    def save_message_to_json(self, message):
        uid = message['client_id']

        self.shared_last_rows[uid] = message
        with FILE_LOCK:
            with open(self.path, 'a') as f:
                json.dump(message, f, indent=4)
                f.write('\n')

    def get_last_row(self, uid):
        return self.shared_last_rows.get(uid, None)
