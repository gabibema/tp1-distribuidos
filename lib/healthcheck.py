from multiprocessing import Manager
import logging
import socket

HEALTHCHECK_PORT = 6666
HEALTHCHECK_BACKLOG = 4


class Health:
    def __init__(self):
        self.manager = Manager()
        self.health = self.manager.Value('b', True)
    
    def set_broken(self):
        self.health.value = False

HEALTH = Health()

class Healthcheck:    
    def __init__(self) -> None:
        self.socket = None

    def listen_healthchecks(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', HEALTHCHECK_PORT))
        self.socket.listen(HEALTHCHECK_BACKLOG)
        try:
            while True:
                logging.warning("Listening for healthchecks")
                conn, addr = self.socket.accept()
                try:
                    conn.sendall(bytes([HEALTH.health.value]))
                except socket.error as e:
                    logging.warning(f"Error sending data: {e}")
                finally:
                    conn.close()
        except Exception as e:
            logging.warning(f"Server error: {e}")
        finally:
            self.socket.close()

