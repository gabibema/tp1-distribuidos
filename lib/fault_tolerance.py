import logging

class duplicateFilterState():
    def __init__(self):
        self.pending = []
        self.max_id = 0

class State():
    def __init__(self):
        self.duplicate_filter = duplicateFilterState()
        pass

    def save():
        pass

    def load():
        pass


def is_duplicate(message_id, state):
    """
    Checks if a message with the same message ID has arrived in the past.
    It also adds the message_id to the state for future verifications.
    """
    if message_id > state.max_id:
        state.pending.extend(pending_id for pending_id in range(state.max_id + 1, message_id))
        state.max_id = message_id
        return False
    else:
        try:
            state.pending.remove(message_id)
            return False
        except ValueError:
            # ID is not pending, message is a duplicate.
            logging.warning(f"Found duplicate message with ID '{message_id}'")
            return True
