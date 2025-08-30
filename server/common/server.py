import socket
import logging

from server.common.messages import BetResponseMessage
from .protocol import Protocol, MessageType, BetMessage
from .utils import Bet, store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._running = True

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._running:
                    logging.error(
                        f"action: accept_connection | result: fail | error: {e}"
                    )
                break

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            logging.info(f"action: accept_bet | result: in_progress | ip: {addr[0]}")

            message = Protocol.receive_message(client_sock)
            if message is None:
                logging.error(
                    f"action: receive_bet | result: fail | ip: {addr[0]} | error: no_message_received"
                )
                return

            try:
                bet = BetMessage.from_bytes(message)
            except Exception as e:
                Protocol.send_message(client_sock, BetResponseMessage.to_bytes(False))
                logging.error(f"action: receive_bet | result: fail | error: {e}")
                return

            store_bets([bet])

            logging.info(
                f"action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}"
            )

            Protocol.send_message(client_sock, BetResponseMessage.to_bytes(True))

            logging.info(f"action: send_bet_response | result: success | ip: {addr[0]}")

        except Exception as e:
            logging.error(f"action: handle_bet | result: fail | error: {e}")
            try:
                Protocol.send_message(client_sock, BetResponseMessage.to_bytes(False))
            except:
                pass
        finally:
            try:
                client_sock.close()
                logging.debug("action: close_client_connection | result: success")
            except OSError as e:
                logging.error(
                    f"action: close_client_connection | result: fail | error: {e}"
                )

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info("action: accept_connections | result: in_progress")
        try:
            c, addr = self._server_socket.accept()
            logging.info(
                f"action: accept_connections | result: success | ip: {addr[0]}"
            )
            return c
        except OSError as e:
            if self._running:
                logging.error(f"action: accept_connections | result: fail | error: {e}")
            return None

    def stop(self):
        logging.info("action: shutdown_server | result: in_progress")
        self._running = False
        try:
            self._server_socket.close()
            logging.info("action: close_server_socket | result: success")
        except OSError as e:
            logging.error(f"action: close_server_socket | result: fail | error: {e}")
        logging.info("action: shutdown_server | result: success")
