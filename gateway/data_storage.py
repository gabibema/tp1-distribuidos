import json
import logging
from multiprocessing import Manager, Lock

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
        self.path = path
        self.mode = mode
        self.__load_from_file()

    def save_message_to_json(self, message):
        self.save_message_in_memory(message)
        with FILE_LOCK:
            with open(self.path, 'a') as f:
                json.dump(message, f, indent=4)
                f.write('\n')
        
    def save_message_in_memory(self, message):
        uid = message['request_id']
        source = message['source']
        self.__create_if_not_exists(uid)

        if self.mode == ALL_ROWS:
            self.shared_rows[uid].append(message)
            logging.warning(f'Added message to {uid}: messages stored: {len(self.shared_rows[uid])}')
        elif self.mode == LATEST_ROW:
            self.shared_rows[uid][source] = message
    
    def __create_if_not_exists(self, uid):
        if self.mode == ALL_ROWS and uid not in self.shared_rows:
            self.shared_rows[uid] = self.manager.list()
        elif self.mode == LATEST_ROW and uid not in self.shared_rows:
            self.shared_rows[uid] = self.manager.dict()
        

    def get_row(self, uid):
        return self.shared_rows.get(uid, None)
    
    def __load_from_file(self):
        with FILE_LOCK:
            # try:
                # with open(self.path, 'r') as f:
                #     content = f.read()
                #     messages = content.split('\n}\n{')
                #     messages = ['{' + msg + '}' if not msg.startswith('{') else msg + '}' if not msg.endswith('}') else msg for msg in messages]
                #     for msg in messages:
                #         logging.warning(f'Loading message: {msg}')
                #         message = json.loads(msg)
                #         if 'request_id' not in message:
                #             continue
                #         self.save_message_in_memory(message)
            with open(self.path, 'r') as f:
                content = f.read()
                # Split the content based on the pattern '\n}\n{'
                messages = content.split('\n}\n{')
                # Adjust the boundaries of each message
                if len(messages) > 1:
                    messages[0] += '}'
                    messages[-1] = '{' + messages[-1]
                    for i in range(1, len(messages) - 1):
                        messages[i] = '{' + messages[i] + '}'
                # Process each message
                for msg in messages:
                    #logging.warning(f'Loading message: {msg}')
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

        try:
            normal_dict = {k: dict(v) for k, v in self.shared_rows.items()}
            logging.warning(f'Loaded {len(self.shared_rows)} entries')
        except ValueError as e:
            logging.error(f'Error converting shared_rows to dictionary: {e}')
            # Attempt a different approach to handle the data if needed
            normal_dict = {k: v for k, v in self.shared_rows.items()}
            logging.warning(f'Loaded {len(self.shared_rows)} entries (without conversion)')
