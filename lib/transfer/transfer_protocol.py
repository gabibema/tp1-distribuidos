from socket import socket
from typing import Tuple
import logging
import struct # for encoding

MESSAGE_FLAG = {
    'BOOK': b'\x01',
    'REVIEW': b'\x02',
    'RESULT': b'\x03',
    'EOF': b'\x04',
    'ERROR': b'\x05'
}

LENGTH_FORMAT = '!L'

class TransferProtocol:
    def __init__(self, conn: socket):
        self.conn = conn

    def send_message(self, message: str):
        """
        Sends a message given a socket connection avoiding short writes
        """
        total_sent = 0
        payload = message.encode('utf-8')
        length = struct.pack(LENGTH_FORMAT, len(payload))
        full_message = length + payload

        try:
            while total_sent < len(full_message):
                sent = self.conn.send(full_message[total_sent:])
                total_sent += sent
        except OSError as e:
            logging.error(f"Error while sending message: {e}")
            return None
        return total_sent


    def read_header(self) -> Tuple[str,str,str]:
        """
        Reads both the initial and final header of a message from a socket in 16-byte blocks,
        ensuring that two delimiters are received to complete the header.
        """
        buffer = b''
        length_header_bytes = struct.calcsize(LENGTH_FORMAT)
        while len(buffer) < length_header_bytes:
            chunk = self.conn.recv(HEADER_CHUNK_SIZE)
            buffer += chunk
        
        length, payload = buffer[:4], buffer[4:]
        return struct.unpack(LENGTH_FORMAT, length)[0], payload



    def receive_message(self) -> Tuple[str,str]:
        """
        Receives a message from a socket avoiding short reads
        """
        length, message = self.read_header()
        while len(message) < length:
            message += self.conn.recv(length - len(message))
        return message.decode()
