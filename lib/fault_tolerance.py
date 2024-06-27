import logging

class duplicateFilterState():
    def __init__(self):
        self.pending = []
        self.max_id = 0

class State():
    def __init__(self):
        self.duplicate_filter = {}
        pass

    def save():
        pass

    def load():
        pass


def is_duplicate(request_id, message_id, state):
    """
    Checks if a message with the same message ID has arrived in the past for that request_id.
    It also updates the state to reflect that the ID has arrived. It does not save the state, thus it needs to be saved outside of this function.    
    """
    client_specific_state = state.get(request_id, duplicateFilterState())
    state[request_id] = client_specific_state
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


def is_repeated(request_id, message_id, state):
    """
    Checks if message_id equals the ID of the last message received.
    Works to detect duplicates coming from the same worker.
    It also updates the state for future validations. It does not save the state, thus it needs to be saved outside of this function.    
    """
    last_message_id = state.get(request_id)
    state[request_id] = message_id
    return message_id == last_message_id
