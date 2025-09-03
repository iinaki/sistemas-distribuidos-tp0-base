import socket
import logging
import struct
import signal
from typing import Optional
from multiprocessing import Process, Lock, RLock, Manager

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
    def __init__(self, port, listen_backlog, expected_agencies):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._expected_agencies = expected_agencies

        self._manager = Manager()
        self._agencies_finished_sending_set = self._manager.dict() 
        self._agencies_finished_sending_lock = Lock()
    
        self._store_bets_lock = Lock() 
        self._read_bets_lock = RLock()
        
        self._processes = []

        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, sig, frame):
        logging.info(f"action: shutdown_signal | signal: {sig}")
        self._running = False
        self._cleanup_processes()

    def run(self):
        try:
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    if client_sock is None or not self._running:
                        break

                    if client_sock:
                        process = Process(
                            target=self.__handle_new_client_process,
                            args=(
                                client_sock,
                                self._agencies_finished_sending_set,
                                self._agencies_finished_sending_lock,   
                                self._expected_agencies,
                                self._store_bets_lock,
                                self._read_bets_lock,
                            ),
                        )
                        process.start()
                        self._processes.append(process)

                        self._cleanup_finished_processes()

                except OSError as e:
                    if self._running:
                        logging.error(
                            f"action: accept_connection | result: fail | error: {e}"
                        )
                    if client_sock is not None:
                        client_sock.close()
                    
                    self._server_socket.close()
                    break
        finally:
            self._cleanup_processes()
            self._server_socket.close()

    def _cleanup_finished_processes(self):
        self._processes = [p for p in self._processes if p.is_alive()]

    def _cleanup_processes(self):
        for process in self._processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
                if process.is_alive():
                    process.kill()
        self._processes.clear()

    @staticmethod
    def __handle_new_client_process(
        client_sock,
        agencies_finished_sending_set,
        agencies_finished_sending_lock,
        expected_agencies,
        store_bets_lock,
        read_bets_lock,
    ):
        try:
            Server.__handle_client_connection(
                client_sock,
                agencies_finished_sending_set,
                agencies_finished_sending_lock,
                expected_agencies,
                store_bets_lock,
                read_bets_lock,
            )
        except Exception as e:
            logging.error(f"action: handle_client_process | result: fail | error: {e}")
        finally:
            try:
                client_sock.close()
                logging.debug("action: close_client_connection | result: success")
            except OSError as e:
                logging.error(
                    f"action: close_client_connection | result: fail | error: {e}"
                )

    @staticmethod
    def __handle_client_connection(
        client_sock,
        agencies_finished_sending_set,
        agencies_finished_sending_lock,
        expected_agencies,
        store_bets_lock,
        read_bets_lock,
    ):
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
                    Server.__handle_bet_message(
                        client_sock,
                        message_bytes,
                        addr,
                        store_bets_lock,
                    )
                elif message_type == MSG_TYPE_FINISHED_SENDING:
                    Server.__handle_finished_sending_message(
                        client_sock,
                        message_bytes,
                        addr,
                        agencies_finished_sending_set,
                        agencies_finished_sending_lock,
                        expected_agencies,
                    )
                elif message_type == MSG_TYPE_WINNERS_REQUEST:
                    Server.__handle_winners_request_message(
                        client_sock,
                        message_bytes,
                        agencies_finished_sending_set,
                        agencies_finished_sending_lock,
                        expected_agencies,
                        read_bets_lock,
                    )
                else:
                    logging.error(
                        f"action: handle_message | result: fail | error: unknown_message_type: {message_type}"
                    )
                    Protocol.send_message(
                        client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
                    )
                    break

        except Exception as e:
            logging.error(f"action: handle_connection | result: fail | error: {e}")
            addr = client_sock.getpeername() if client_sock else "unknown"
            logging.info(f"action: connection_ended | result: fail | address: {addr}")

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

    @staticmethod
    def __handle_bet_message(
        client_sock, message_bytes, addr, store_bets_lock
    ):
        try:
            batch_bets = Server.parse_batch_bet_message(message_bytes)
        except Exception as e:
            logging.error(f"action: parse_bet | result: fail | error: {e}")
            Protocol.send_message(
                client_sock, MSG_TYPE_BET, BetResponseMessage.to_bytes(False)
            )
            return

        bets_len = len(batch_bets)

        try:
            Server.process_successful_batch_bets(
                batch_bets, client_sock, bets_len, store_bets_lock
            )
        except Exception as e:
            Server.process_failed_batch_bets(bets_len, client_sock, e)
            return

        logging.info(f"action: send_bet_response | result: success | ip: {addr[0]}")

    @staticmethod
    def __handle_finished_sending_message(
        client_sock,
        message_bytes,
        addr,
        agencies_finished_sending_set,
        agencies_finished_sending_lock,
        expected_agencies,
    ):
        try:
            agency_id = FinishedSendingMessage.from_bytes(message_bytes)
            logging.info(
                f"action: finished_sending_received | result: success | ip: {addr[0]} | agency_id: {agency_id}"
            )

            with agencies_finished_sending_lock:
                agencies_finished_sending_set[agency_id] = True

            Protocol.send_message(
                client_sock,
                MSG_TYPE_FINISHED_SENDING,
                BetResponseMessage.to_bytes(True),
            )

            logging.info(
                f"action: agency_finished_registered | result: success | agency_id: {agency_id} | agencies_finished: {len(agencies_finished_sending_set)} | total_participating_agencies: {expected_agencies}"
            )

        except Exception as e:
            logging.error(
                f"action: handle_finished_sending | result: fail | error: {e}"
            )
            Protocol.send_message(
                client_sock,
                MSG_TYPE_FINISHED_SENDING,
                BetResponseMessage.to_bytes(False),
            )
    
    @staticmethod
    def get_lottery_winners(agency_id, read_bets_lock):
        try:
            logging.info("action: execute_lottery | result: in_progress")

            with read_bets_lock:
                all_bets = list(load_bets())
                logging.info(
                    f"action: load_all_bets | result: success | total_bets: {len(all_bets)}"
                )

            agency_winners = []
            for bet in all_bets:
                if has_won(bet) and bet.agency == int(agency_id):
                    agency_winners.append(bet.document)

            return agency_winners
        
        except Exception as e:
            logging.error(f"action: execute_lottery | result: fail | error: {e}")

    @staticmethod
    def __handle_winners_request_message(
        client_sock, 
        message_bytes, 
        agencies_finished_sending_set,
        agencies_finished_sending_lock,
        expected_agencies,
        read_bets_lock,
    ):
        try:
            agency_id = WinnersRequestMessage.from_bytes(message_bytes)
            logging.info(
                f"action: winners_request_received | result: success | agency_id: {agency_id}"
            )

            with agencies_finished_sending_lock: 
                if len(agencies_finished_sending_set) < expected_agencies:
                    logging.warning(
                        f"action: sending_lottery_not_ready | result: success"
                    )
                    empty_response = WinnersResponseMessage.to_bytes([])
                    Protocol.send_message(
                        client_sock, MSG_TYPE_LOTTERY_NOT_READY, empty_response
                    )
                else:
                    agency_winners = Server.get_lottery_winners(agency_id, read_bets_lock)
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

        self._cleanup_processes()

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
        batch_bets: list[Bet],
        client_sock: socket.socket,
        bets_len: int,
        store_bets_lock, 
    ):
        with store_bets_lock:
            store_bets(batch_bets)

        # logging.info(
        #     f"action: apuesta_recibida | result: success | cantidad: {bets_len}"
        # )

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
