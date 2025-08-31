import socket
import logging
import struct
from typing import Optional

from .messages import (
    BetMessage,
    BetResponseMessage,
    FinishedSendingMessage,
    WinnersRequestMessage,
    WinnersResponseMessage,
    MSG_TYPE_BET,
    MSG_TYPE_FINISHED_SENDING,
    MSG_TYPE_WINNERS_REQUEST,
    MSG_TYPE_WINNERS_RESPONSE,
    MSG_TYPE_LOTTERY_NOT_READY,
)
from .protocol import Protocol
from .utils import store_bets, Bet, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._running = True

        self._agencies_finished = set()
        self._lottery_executed = False
        self._winning_number = None
        self._winners = []
        self._max_agency_id = 0 

    def _execute_lottery(self):
        if self._lottery_executed:
            return

        try:
            logging.info("action: execute_lottery | result: in_progress")

            all_bets = list(load_bets())
            logging.info(
                f"action: load_all_bets | result: success | total_bets: {len(all_bets)}"
            )

            winners = []
            for bet in all_bets:
                if has_won(bet):
                    winner_info = {
                        "name": f"{bet.first_name} {bet.last_name}",
                        "document": bet.document,
                        "number": bet.number,
                        "agency": bet.agency,
                    }
                    winners.append(winner_info)

            self._winners = winners
            self._lottery_executed = True

            logging.info(
                f"action: execute_lottery | result: success | winners_count: {len(winners)}"
            )

        except Exception as e:
            logging.error(f"action: execute_lottery | result: fail | error: {e}")
            # Keep lottery as not executed so it can be retried
            self._lottery_executed = False

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
        try:
            addr = client_sock.getpeername()
            logging.info(f"action: accept_bet | result: in_progress")

            while True:
                message_result = Protocol.receive_message(client_sock)
                if message_result is None:
                    logging.info(f"action: client_disconnected | result: success")
                    break

                message_type, message_bytes = message_result

                if message_type == MSG_TYPE_BET:
                    self.__handle_bet_message(client_sock, message_bytes, addr)
                elif message_type == MSG_TYPE_FINISHED_SENDING:
                    self.__handle_finished_sending_message(
                        client_sock, message_bytes, addr
                    )
                elif message_type == MSG_TYPE_WINNERS_REQUEST:
                    self.__handle_winners_request_message(client_sock, message_bytes)
                    break
                else:
                    logging.error(
                        f"action: handle_message | result: fail | error: unknown_message_type: {message_type}"
                    )
                    Protocol.send_message(
                        client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
                    )
                    break

        except Exception as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            try:
                Protocol.send_message(
                    client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
                )
            except:
                pass
        finally:
            try:
                client_sock.close()
                if addr:
                    logging.debug(
                        f"action: close_client_connection | result: success | ip: {addr[0]}"
                    )
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

    def __handle_bet_message(self, client_sock, message_bytes, addr):
        try:
            batch_bets = Server.parse_batch_bet_message(message_bytes)
        except Exception as e:
            logging.error(f"action: parse_bet | result: fail | error: {e}")
            Protocol.send_message(
                client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
            )
            return

        self._max_agency_id = max(self._max_agency_id, batch_bets[0].agency)

        bets_len = len(batch_bets)

        try:
            Server.process_successful_batch_bets(batch_bets, client_sock, bets_len)
        except Exception as e:
            Server.process_failed_batch_bets(bets_len, client_sock, e)
            return

        logging.info(f"action: send_bet_response | result: success | ip: {addr[0]}")

    def __handle_finished_sending_message(self, client_sock, message_bytes, addr):
        try:
            agency_id = FinishedSendingMessage.from_bytes(message_bytes)
            logging.info(
                f"action: finished_sending_received | result: success | ip: {addr[0]} | agency_id: {agency_id}"
            )

            self._agencies_finished.add(agency_id)
            logging.info(
                f"action: agency_finished_registered | result: success | agency_id: {agency_id} | agencies_finished: {len(self._agencies_finished)} | max_agency_id: {self._max_agency_id}"
            )

            Protocol.send_message(
                client_sock,
                MSG_TYPE_FINISHED_SENDING,
                BetResponseMessage.to_bytes(True),
            )

            if len(self._agencies_finished) >= self._max_agency_id and not self._lottery_executed:
                logging.info(
                    f"action: all_agencies_finished | result: success | total_agencies: {self._max_agency_id} | executing_lottery"
                )
                self._execute_lottery()

        except Exception as e:
            logging.error(
                f"action: handle_finished_sending | result: fail | error: {e}"
            )
            Protocol.send_message(
                client_sock,
                MSG_TYPE_FINISHED_SENDING,
                BetResponseMessage.to_bytes(False),
            )

    def __handle_winners_request_message(self, client_sock, message_bytes):
        try:
            agency_id = WinnersRequestMessage.from_bytes(message_bytes)
            logging.info(
                f"action: winners_request_received | result: success | agency_id: {agency_id}"
            )

            if not self._lottery_executed:
                logging.warning(
                    "action: winners_request | result: lottery_not_executed_yet"
                )
                empty_response = WinnersResponseMessage.to_bytes([])
                Protocol.send_message(
                    client_sock, MSG_TYPE_LOTTERY_NOT_READY, empty_response
                )
            else:
                agency_winners = [
                    bet["document"]
                    for bet in self._winners
                    if bet["agency"] == int(agency_id)
                ]
                logging.info(
                    f"action: winners_retrieved | result: success | agency_id: {agency_id} | winners_count: {len(agency_winners)}"
                )
                response = WinnersResponseMessage.to_bytes(agency_winners)
                Protocol.send_message(client_sock, MSG_TYPE_WINNERS_RESPONSE, response)

        except Exception as e:
            logging.error(f"action: handle_winners_request | result: fail | error: {e}")
            empty_response = WinnersResponseMessage.to_bytes([])
            Protocol.send_message(
                client_sock, MSG_TYPE_WINNERS_RESPONSE, empty_response
            )

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

            individual_length, is_last_bet_flag = struct.unpack(
                "!IB", message_bytes[offset : offset + 5]
            )
            is_last_bet = bool(is_last_bet_flag)

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

        Protocol.send_message(
            client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(True)
        )

    @staticmethod
    def process_failed_batch_bets(
        bets_len: int, client_sock: socket.socket, e: Exception
    ):
        logging.error(
            f"action: apuesta_recibida | result: fail | cantidad: {bets_len} | error: {e}"
        )
        Protocol.send_message(
            client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
        )
