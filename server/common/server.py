import socket
import logging
import signal


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._running = True

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logging.info(f"action: receive_signal | result: success | signal: {signum}")
        self.stop()

    def run(self):
        """
        Server loop with graceful shutdown support

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        logging.info("action: server_start | result: success")
        try:
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
        finally:
            if self._running:
                self.stop()
            logging.info("action: server_finished | result: success")

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode("utf-8")
            addr = client_sock.getpeername()
            logging.info(
                f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
            )
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode("utf-8"))
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
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
            self._server_socket.shutdown(socket.SHUT_RDWR)
            logging.debug("action: shutdown_server_socket | result: success")
        except OSError as e:
            logging.debug(f"action: shutdown_server_socket | result: fail | error: {e}")

        try:
            self._server_socket.close()
            logging.info("action: close_server_socket | result: success")
        except OSError as e:
            logging.error(f"action: close_server_socket | result: fail | error: {e}")

        logging.info("action: shutdown_server | result: success")
