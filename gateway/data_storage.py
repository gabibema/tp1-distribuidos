import json
from multiprocessing import Manager, Lock

FILE_LOCK = Lock()

class DataSaver:
    def __init__(self, path):
        self.manager = Manager()
        self.shared_last_rows = self.manager.dict()
        self.path = path

    def save_message_to_json(self, uid, last_row):
        values = ",".join(last_row.values())
        data = {uid: values}
        self.shared_last_rows[uid] = values  # Guardar en el diccionario compartido
        with FILE_LOCK:
            with open(self.path, 'a') as f:
                json.dump(data, f, indent=4)

    def get_last_row(self, uid):
        return self.shared_last_rows.get(uid, None)
