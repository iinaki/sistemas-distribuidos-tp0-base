package common

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchMaxAmount int
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

func (c *Client) loadBetsFromCSV() ([]Bet, error) {
	file, err := os.Open("/agency.csv")
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV file: %v", err)
	}

	var bets []Bet
	for _, record := range records {
		if len(record) != 5 {
			log.Errorf("invalid CSV record: %v", record)
			continue
		}
		bet := Bet{
			AgencyID:   c.config.ID,
			Nombre:     record[0],
			Apellido:   record[1],
			Documento:  record[2],
			Nacimiento: record[3],
			Numero:     record[4],
		}
		bets = append(bets, bet)
	}

	return bets, nil
}

// Creamos Bet: len + is_last_bet + betmessage
func (c *Client) createBetMessage(bet Bet, isLastBet bool) []byte {
	betMessageBytes := bet.toBytes()

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header, uint32(len(betMessageBytes)))

	if isLastBet {
		header[4] = 1
	} else {
		header[4] = 0
	}

	result := make([]byte, 0, len(header)+len(betMessageBytes))
	result = append(result, header...)
	result = append(result, betMessageBytes...)

	return result
}

func (c *Client) SendBatch(protocol *Protocol, bets []Bet) error {
	var batchMessage []byte

	for i, bet := range bets {
		isLastBet := (i == len(bets)-1)
		betMessage := c.createBetMessage(bet, isLastBet)
		batchMessage = append(batchMessage, betMessage...)
	}

	log.Debugf("action: send_batch | result: in_progress | client_id: %v | batch_size: %d | total_bytes: %d",
		c.config.ID, len(bets), len(batchMessage))

	return protocol.SendMessage(c.conn, batchMessage)
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

// StartClientLoop: Loads bets from CSV and sends them in batches to the server
func (c *Client) StartClientLoop() {
	bets, err := c.loadBetsFromCSV()
	if err != nil {
		log.Errorf("action: load_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: load_bets | result: success | client_id: %v | total_bets: %d", c.config.ID, len(bets))

	protocol := NewProtocol()
	err = c.createClientSocket()
	if err != nil {
		log.Errorf("action: create_socket | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return
	}

	batchCount := 0

	for i := 0; i < len(bets) && c.running; i += c.config.BatchMaxAmount {
		batchCount++

		end := i + c.config.BatchMaxAmount
		if end > len(bets) {
			end = len(bets)
		}

		batch := bets[i:end]

		err = c.SendBatch(protocol, batch)
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %d | error: %v",
				c.config.ID, batchCount, err)
			c.closeConnection()
			return
		}

		responseBytes, err := protocol.ReceiveMessage(c.conn)
		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | batch_id: %d | error: %v",
				c.config.ID, batchCount, err)
			c.closeConnection()
			return
		}

		success, err := ParseBetResponse(responseBytes)
		if err != nil {
			log.Errorf("action: parse_response | result: fail | client_id: %v | batch_id: %d | error: %v",
				c.config.ID, batchCount, err)
			c.closeConnection()
			return
		}

		if success {
			log.Infof("action: apuesta_recibida | result: success | client_id: %v | batch_id: %d | cantidad: %d",
				c.config.ID, batchCount, len(batch))
		} else {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | batch_id: %d | cantidad: %d",
				c.config.ID, batchCount, len(batch))
		}

		// // Wait a time between sending one message and the next one
		// if end < len(bets) && c.running {
		// 	time.Sleep(c.config.LoopPeriod)
		// }
	}
	c.closeConnection()

	log.Infof("action: loop_finished | result: success | client_id: %v | total_batches: %d", c.config.ID, batchCount)
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
