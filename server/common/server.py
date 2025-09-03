import socket
import logging
from typing import Optional

from .messages import BetMessage, BetResponseMessage
from .protocol import Protocol
from .utils import store_bets, Bet


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

            while True:
                batch_message_bytes = Protocol.receive_message(client_sock)
                if batch_message_bytes is None:
                    logging.info(f"action: close_client_connection | result: success | ip: {addr[0]}")
                    break

                try:
                    batch_bets = Server.parse_batch_bet_message(batch_message_bytes)
                except Exception as e:
                    logging.error(f"action: parse_bet | result: fail | error: {e}")
                    Protocol.send_message(client_sock, BetResponseMessage.to_bytes(False))
                    break

                bets_len = len(batch_bets)

                try:
                    Server.process_successful_batch_bets(
                        batch_bets, client_sock, bets_len
                    )
                except Exception as e:
                    Server.process_failed_batch_bets(bets_len, client_sock, e)
                    break

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
                logging.error(f"action: close_client_connection | result: fail | error: {e}")

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

    @staticmethod
    def parse_individual_bet_message(
        message_bytes: bytes, offset: int
    ) -> tuple[Optional[bytes], bool, int]:
        try:
            if offset + 5 > len(message_bytes):
                return None, False, offset

            individual_length = int.from_bytes(
                message_bytes[offset:offset + 4], byteorder="big", signed=False
            )
            is_last_bet_flag = message_bytes[offset + 4]
            is_last_bet = (is_last_bet_flag != 0)

            content_start = offset + 5
            content_end = content_start + individual_length

            if content_end > len(message_bytes):
                return None, False, offset

            individual_content = message_bytes[content_start:content_end]

            return individual_content, is_last_bet, content_end

        except Exception as e:
            logging.error(
                f"action: receive_individual_message | result: fail | error: {e}"
            )
            return None, False, offset

    @staticmethod
    def parse_batch_bet_message(batch_message_bytes: bytes):
        batch_bets = []
        offset = 0
        is_last_bet = False

        while offset < len(batch_message_bytes) and not is_last_bet:
            individual_content, is_last_bet, new_offset = (
                Server.parse_individual_bet_message(batch_message_bytes, offset)
            )

            if individual_content is None:
                logging.error(
                    f"action: parse_individual_message | result: fail | offset: {offset}"
                )
                break

            try:
                bet = BetMessage.from_bytes(individual_content)
                logging.debug(
                    f"action: bet_parsed | result: success | bet: {bet.first_name} {bet.last_name}"
                )
                batch_bets.append(bet)

            except Exception as e:
                raise e

            offset = new_offset

        return batch_bets

    @staticmethod
    def process_successful_batch_bets(
        batch_bets: list[Bet], client_sock: socket.socket, bets_len: int
    ):
        store_bets(batch_bets)

        logging.info(
            f"action: apuesta_recibida | result: success | cantidad: {bets_len}"
        )

        for bet in batch_bets:
            logging.info(
                f"action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}"
            )

        Protocol.send_message(client_sock, BetResponseMessage.to_bytes(True))

    @staticmethod
    def process_failed_batch_bets(
        bets_len: int, client_sock: socket.socket, e: Exception
    ):
        logging.error(
            f"action: apuesta_recibida | result: fail | cantidad: {bets_len} | error: {e}"
        )
        Protocol.send_message(client_sock, BetResponseMessage.to_bytes(False))
