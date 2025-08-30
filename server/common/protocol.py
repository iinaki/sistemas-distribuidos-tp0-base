import json
import struct
import socket
import logging
from typing import Dict, Any, Optional


class Protocol:
    @staticmethod
    def send_message(sock: socket.socket, message: Dict[str, Any]) -> None:
        try:
            json_data = json.dumps(message).encode("utf-8")

            header = struct.pack("!I", len(json_data))

            full_message = header + json_data

            Protocol._send_exact(sock, full_message)

            logging.debug(
                f"action: send_message | result: success | bytes_sent: {bytes_sent}"
            )

        except Exception as e:
            logging.error(f"action: send_message | result: fail | error: {e}")
            raise

    @staticmethod
    def receive_message(sock: socket.socket) -> Optional[Dict[str, Any]]:
        try:
            header_data = Protocol._receive_exact(sock, 4)
            if not header_data:
                return None

            message_length = struct.unpack("!I", header_data)[0]

            json_data = Protocol._receive_exact(sock, message_length)
            if not json_data:
                return None

            message = json.loads(json_data.decode("utf-8"))

            logging.debug(
                f"action: receive_message | result: success | message_length: {message_length}"
            )
            return message

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
        bytes_sent = 0
        while bytes_sent < len(full_message):
            sent = sock.send(full_message[bytes_sent:])
            if sent == 0:
                raise ConnectionError("Socket connection broken")
            bytes_sent += sent
