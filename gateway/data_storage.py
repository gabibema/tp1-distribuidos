import csv
import io
import json
import logging
from multiprocessing import Manager, Lock
from lib.fault_tolerance import State, is_duplicate

FILE_LOCK = Lock()
LATEST_ROW = 0
ALL_ROWS = 1

class DataSaver:
    """
    Class responsible for saving messages to a file and loading them from it.
    -path: path to the file where the messages are saved (persistent storage)
    -mode: mode in which messages are stored in memory to use in the program (can be shared between processes)
           if no mode is specified, only the last message is stored in memory
    """
    def __init__(self, path, mode=LATEST_ROW):
        self.manager = Manager()
        self.shared_rows = self.manager.dict()
        self.eof_data = self.manager.dict()
        self.states = self.manager.dict()
        self.path = path
        self.mode = mode
        self.__load_from_file()
    
    def message_duplicate(self, message):
        request_id = message['request_id']
        message_id = message['message_id']
        source = message['source']
        if not self.states.get(source):
            #logging.warning(f'No state found for source {source} creating new state')
            self.states[source] = State()
        return is_duplicate(request_id, message_id, self.states[source].duplicate_filter)

    def save_message_to_json(self, message):
        if self.message_duplicate(message):
            return
        self.save_message_in_memory(message)
        with FILE_LOCK:
            with open(self.path, 'a') as f:
                json.dump(message, f, indent=4)
                f.write('\n')
    
    def save_eof(self, uid, source):
        uid = str(uid)
        if not self.eof_data.get(uid):
            self.eof_data[uid] = 0
        
        self.eof_data[uid] += 1

    def save_message_in_memory(self, message):
        if self.message_duplicate(message):
            return
        message['request_id'] = str(message['request_id'])
        uid = message['request_id']
        source = message['source']
        self.__create_if_not_exists(uid)
        if message.get('eof', False):
            self.save_eof(uid, source)

        if self.mode == ALL_ROWS:
            self.shared_rows[uid].append(message)
        elif self.mode == LATEST_ROW:
            self.shared_rows[uid][source] = message
    
    def __create_if_not_exists(self, uid):
        if self.mode == ALL_ROWS and uid not in self.shared_rows:
            self.shared_rows[uid] = self.manager.list()
        elif self.mode == LATEST_ROW and uid not in self.shared_rows:
            self.shared_rows[uid] = self.manager.dict()
        
    def __load_from_file(self):
        with FILE_LOCK:

            with open(self.path, 'r') as f:
                content = f.read()
                messages = content.split('\n}\n{')
                if len(messages) > 1:
                    messages[0] += '}'
                    messages[-1] = '{' + messages[-1]
                    for i in range(1, len(messages) - 1):
                        messages[i] = '{' + messages[i] + '}'
                for msg in messages:
                    try:
                        message = json.loads(msg)
                        if 'request_id' not in message:
                            continue
                        self.save_message_in_memory(message)
                    except json.JSONDecodeError as e:
                        logging.error(f'Error while decoding JSON from message: {msg}\nError: {e}')
            # except FileNotFoundError:
            #     logging.warning(f'File not found: {self.path}')
            # except json.JSONDecodeError:
            #     logging.error(f'Error while decoding JSON from file: {self.path}')

    def get(self, uid):
        uid = str(uid)
        return self.shared_rows.get(uid, {})
    
    def get_eof_count(self, uid):
        return self.eof_data.get(str(uid), 0)

def write_csv_to_string(headers, rows):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    if headers:
        writer.writerow(headers)
    if rows:
        writer.writerows(rows)
    return output.getvalue()