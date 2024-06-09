from multiprocessing import Manager
from socket import socket
from typing import Tuple
import logging
import struct # for encoding
import uuid

MESSAGE_FLAG = {
    'BOOK': 1,
    'REVIEW': 2,
    'RESULT': 3,
    'EOF': 4,
    'ERROR': 5
}

BYTES_HEADER_SIZE = 4
MESSAGE_HEADER_SIZE = 1 + 16 + 4 # flag + client_uuid + message_id.

class ByteTransferProtocol:
    def __init__(self, conn: socket):
        self.conn = conn

    def send_bytes(self, payload: bytes):
        """
        Sends bytes given a socket connection avoiding short writes
        """
        total_sent = 0
        length = struct.pack('!L', len(payload))
        full_message = length + payload

        try:
            while total_sent < len(full_message):
                sent = self.conn.send(full_message[total_sent:])
                total_sent += sent
        except OSError as e:
            logging.error(f"Error while sending message: {e}")
            return None
        return total_sent

    def read_header(self) -> Tuple[int,bytes]:
        """
        Reads the header indicating the amount of bytes that follow.
        """
        buffer = b''
        while len(buffer) < BYTES_HEADER_SIZE:
            chunk = self.conn.recv(BYTES_HEADER_SIZE - len(buffer))
            buffer += chunk
        
        length, payload = buffer[:4], buffer[4:]
        return struct.unpack('!L', length)[0], payload

    def receive_bytes(self) -> bytes:
        """
        Receives bytes from a socket avoiding short reads
        """
        length, buffer = self.read_header()
        while len(buffer) < length:
            buffer += self.conn.recv(length - len(buffer))
        return buffer


class MessageTransferProtocol(ByteTransferProtocol):
    def send_message(self, flag: int, client_id: uuid.UUID, message_id: int, message: str):
        """
        Sends a message given a socket connection.
        """
        payload = struct.pack('!B', flag) + client_id.bytes + struct.pack('!L', message_id) + message.encode('utf-8')
        return super().send_bytes(payload)

    def receive_message(self) -> Tuple[bytes,uuid.UUID,int,str]:
        """
        Receives a message from a socket.
        """
        payload = super().receive_bytes()
        flag, client_id, message_id, message = payload[0], payload[1:17], payload[17:21], payload[21:]
        return flag, uuid.UUID(bytes=client_id), struct.unpack('!L', message_id), message.decode('utf-8')


class RouterProtocol:
    def __init__(self) -> None:
        manager = Manager()
        self.router = manager.dict()

    def add_connection(self, connection:MessageTransferProtocol, uid: uuid.UUID) -> None:
        self.router[uid] = connection
    
    def send_message(self, flag: int, client_id: uuid.UUID, message_id: int, message: str):
        self.router[client_id].send_message(flag, client_id, message_id, message)
