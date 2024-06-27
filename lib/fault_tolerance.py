import logging
import json
import os

class requestDuplicateFilter():
    def __init__(self):
        self.pending = []
        self.max_id = 0


class DuplicateFilterState():
    def __init__(self):
        self.duplicate_filter = {}

    def __setitem__(self, key, item):
        self.duplicate_filter[key] = item

    def __getitem__(self, key):
        client_specific_state = self.duplicate_filter.get(request_id, requestDuplicateFilter())
        self.duplicate_filter[request_id] = client_specific_state
        return client_specific_state

    def is_duplicate(request_id, message_id):
        """
        Checks if a message with the same message ID has arrived in the past for that request_id.
        It also updates the state to reflect that the ID has arrived. It does not save the state, thus it needs to be saved outside of this function.    
        """
        client_specific_state = self[request_id]
        if message_id > client_specific_state.max_id:
            client_specific_state.pending.extend(pending_id for pending_id in range(client_specific_state.max_id + 1, message_id))
            client_specific_state.max_id = message_id
            return False
        else:
            try:
                client_specific_state.pending.remove(message_id)
                return False
            except ValueError:
                # ID is not pending, message is a duplicate.
                logging.warning(f"Found duplicate message with ID '{message_id}'")
                return True


class DumbDuplicateFilterState():
    def __init__(self):
        self.duplicate_filter = {}

    def is_repeated(request_id, message_id):
        """
        Checks if message_id equals the ID of the last message received.
        Works to detect duplicates coming from the same worker.
        It also updates the state for future validations. It does not save the state, thus it needs to be saved outside of this function.    
        """
        last_message_id = self.duplicate_filter.get(request_id)
        self.duplicate_filter[request_id] = message_id
        return message_id == last_message_id


def save_state(**kwargs):
    "Saves whatever data the worker needs to be atomically saved."
    state = json.dumps(kwargs)
    with open('/var/temp_worker_state', 'w') as statefile:
        statefile.write(state)
    os.replace('/var/temp_worker_state', '/var/worker_state')


def load_state():
    return {}
