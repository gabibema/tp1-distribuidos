from socket import socket
import logging
from typing import Tuple

MESSAGE_FLAG = {
    'BOOK': '1',
    'REVIEW': '2',
    'RESULT': '3',
    'EOF': '4',
    'ERROR': '5'
}

SIZE_DELIMETER = ';'
HEADER_DELIMETER = '|'
HEADER_CHUNK_SIZE = 16

class TransferProtocol:
    def __init__(self, conn: socket):
        self.conn = conn

    def send_message(self, message: str, flag: str):
        """
        Sends a message given a socket connection avoiding short writes
        """
        total_sent = 0
        full_message = f"{HEADER_DELIMETER}{len(message)}{SIZE_DELIMETER}{flag}{HEADER_DELIMETER}{message}".encode()

        try:
            while total_sent < len(full_message):
                sent = self.conn.send(full_message[total_sent:])
                total_sent += sent
        except OSError as e:
            logging.error(f"Error while sending message: {e}")
            return None
        return total_sent


    def __read_header(self) -> Tuple[str,str,str]:
        """
        Reads both the initial and final header of a message from a socket in 16-byte blocks,
        ensuring that two delimiters are received to complete the header.
        """
        buffer = b''
        while True:
            chunk = self.conn.recv(HEADER_CHUNK_SIZE)
            buffer += chunk
            if buffer.count(HEADER_DELIMETER.encode()) >= 2:
                break
        
        header_and_message = buffer.decode()
        try:
            _, header_part, message_part = header_and_message.split(HEADER_DELIMETER, 2)
            size, flag = header_part.split(SIZE_DELIMETER)
        except ValueError as e:
            raise ValueError(f"Error parsing header: expected format not found. {str(e)}")
        
        return (size, flag, message_part)



    def receive_message(self) -> Tuple[str,str]:
        """
        Receives a message from a socket avoiding short reads
        """
        try: 
            size, flag, message = self.__read_header()
        except ValueError:
            return ("", MESSAGE_FLAG["ERROR"])

        size = int(size) 
        while len(message) < size:
            message += self.conn.recv(size - len(message)).decode()
        return (message, flag) 
