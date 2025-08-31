package common

import (
	"net"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	Nombre        string
	Apellido      string
	Documento     string
	Nacimiento    string
	Numero        string
}

// Client Entity that encapsulates how
type Client struct {
	config  ClientConfig
	conn    net.Conn
	running bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:  config,
		running: true,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and error is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send Bet messages to the server
func (c *Client) StartClientLoop() {
	protocol := NewProtocol()

	for msgID := 1; msgID <= c.config.LoopAmount && c.running; msgID++ {
		err := c.createClientSocket()
		if err != nil {
			log.Errorf("action: create_socket | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		betMessageBytes := CreateBetMessage(
			c.config.ID,
			c.config.Nombre,
			c.config.Apellido,
			c.config.Documento,
			c.config.Nacimiento,
			c.config.Numero,
		)

		err = protocol.SendMessage(c.conn, betMessageBytes)
		if err != nil {
			log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			c.closeConnection()
			return
		}

		responseBytes, err := protocol.ReceiveMessage(c.conn)
		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			c.closeConnection()
			return
		}

		success, err := ParseBetResponse(responseBytes)
		if err != nil {
			log.Errorf("action: parse_response | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			c.closeConnection()
			return
		}

		if success {
			log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s",
				c.config.Documento,
				c.config.Numero,
			)
		} else {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: server_returned_error",
				c.config.ID,
			)
		}

		c.closeConnection()

		// Wait a time between sending one message and the next one
		if msgID < c.config.LoopAmount && c.running {
			time.Sleep(c.config.LoopPeriod)
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

func (c *Client) closeConnection() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			log.Errorf("action: close_connection | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
		} else {
			log.Debugf("action: close_connection | result: success | client_id: %v", c.config.ID)
		}
		c.conn = nil
	}
}

func (c *Client) Stop() {
	log.Infof("action: shutdown_client | result: in_progress | client_id: %v", c.config.ID)
	c.running = false
	c.closeConnection()
	log.Infof("action: shutdown_client | result: success | client_id: %v", c.config.ID)
}
