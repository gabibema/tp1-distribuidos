from socket import socket
from typing import Tuple
import logging
import struct # for encoding
import uuid

MESSAGE_FLAG = {
    'BOOK': b'\x01',
    'REVIEW': b'\x02',
    'RESULT': b'\x03',
    'EOF': b'\x04',
    'ERROR': b'\x05'
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
            chunk = self.conn.recv(HEADER_CHUNK_SIZE)
            buffer += chunk
        
        length, payload = buffer[:4], buffer[4:]
        return struct.unpack('!L', length)[0], payload

    def receive_bytes(self) -> bytes:
        """
        Receives bytes from a socket avoiding short reads
        """
        length, payload = self.read_header()
        while len(payload) < length:
            payload += self.conn.recv(length - len(payload))
        return payload


class MessageTransferProtocol(ByteTransferProtocol):
    def send_message(self, flag: bytes, client_id: uuid.UUID, message_id: int, message: str):
        """
        Sends a message given a socket connection.
        """
        payload = flag + client_id.bytes + struct.pack('!L', message_id) + message.encode('utf-8')
        return super().send_bytes(payload)

    def receive_message(self) -> Tuple[bytes,uuid.UUID,int,str]:
        """
        Receives a message from a socket.
        """
        payload = super().receive_bytes()
        flag, client_id, message_id, message = payload[0], payload[1:17], payload[17:21], payload[21:]
        return flag, uuid.UUID(bytes=client_id), struct.unpack('!L', message_id), message.decode('utf-8')
