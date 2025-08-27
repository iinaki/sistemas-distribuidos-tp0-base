import json
import struct
import socket
import logging
from typing import Dict, Any, Optional


class Protocol:
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
