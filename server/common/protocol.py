import struct
import socket
import logging
from typing import Optional, Tuple

HEADER_LEN = 5  # 4 bytes para length + 1 byte para message_type
MAX_MESSAGE_LEN = 8 * 1024 # 8kb

class Protocol:
    @staticmethod
    def send_message(sock: socket.socket, message_type: int, message_bytes: bytes) -> None:
        try:
            length = len(message_bytes)
            if length > MAX_MESSAGE_LEN:
                raise ValueError(f"message too large: {length} bytes")

            # 4 bytes de len + 1 byte del message type
            header = length.to_bytes(4, byteorder="big", signed=False) + bytes([message_type])
            full_message = header + message_bytes

            Protocol._send_exact(sock, full_message)

        except Exception as e:
            logging.error(f"action: send_message | result: fail | error: {e}")
            raise

    @staticmethod
    def receive_message(sock: socket.socket) -> Optional[Tuple[int, bytes]]:
        try:
            header = Protocol._receive_exact(sock, HEADER_LEN)
            if not header:
                return None

            message_length = int.from_bytes(header[:4], byteorder="big", signed=False)
            message_type = header[4]

            if message_length < 0 or message_length > MAX_MESSAGE_LEN:
                logging.error(
                    f"action: receive_message | result: fail | error: invalid_length={message_length}"
                )
                return None

            message_bytes = Protocol._receive_exact(sock, message_length)

            if message_bytes is None:
                return None

            return (message_type, message_bytes)

        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            return None

    @staticmethod
    def _receive_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
        data = b""
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    @staticmethod
    def _send_exact(sock: socket.socket, full_message: bytes) -> None:
        total_len = len(full_message)
        bytes_sent = 0
        while bytes_sent < total_len:
            sent = sock.send(full_message[bytes_sent:])
            if sent == 0:
                raise ConnectionError("Socket connection broken")
            bytes_sent += sent

            logging.debug(
                f"action: send_message | result: success | bytes_sent: {bytes_sent}"
            )
