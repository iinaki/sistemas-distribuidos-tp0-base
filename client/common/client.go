package common

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
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

func (c *Client) receiveBetResponse(protocol *Protocol, batchID int, batchSize int) error {
	messageType, responseBytes, err := protocol.ReceiveMessage(c.conn)
	if err != nil {
		log.Errorf("action: receive_response | result: fail | client_id: %v | batch_id: %d | error: %v",
			c.config.ID, batchID, err)
		c.closeConnection()
		return err
	}

	if messageType != MsgTypeBet {
		log.Errorf("action: receive_response | result: fail | client_id: %v | batch_id: %d | error: unexpected_message_type: %d",
			c.config.ID, batchID, messageType)
		c.closeConnection()
		return fmt.Errorf("unexpected message type: %d", messageType)
	}

	success, err := BetResponseFromBytes(responseBytes)
	if err != nil {
		log.Errorf("action: parse_response | result: fail | client_id: %v | batch_id: %d | error: %v",
			c.config.ID, batchID, err)
		c.closeConnection()
		return err
	}

	if success {
		log.Infof("action: apuesta_recibida | result: success | client_id: %v | batch_id: %d | cantidad: %d",
			c.config.ID, batchID, batchSize)
	} else {
		log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | batch_id: %d | cantidad: %d",
			c.config.ID, batchID, batchSize)
	}
	return nil
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

	return protocol.SendMessage(c.conn, MsgTypeBet, batchMessage)
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
	err := c.createClientSocket()
	if err != nil {
		log.Errorf("action: create_persistent_socket | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer c.closeConnection()

	protocol := NewProtocol()

	err = c.sendBets(protocol)
	if err != nil {
		log.Errorf("action: send_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	err = c.sendFinishedSending(protocol)
	if err != nil {
		log.Errorf("action: send_finished_sending | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	err = c.requestWinners(protocol)
	if err != nil {
		log.Errorf("action: request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: client_finished | result: success")
}

func (c *Client) closeConnection() {
	if c.conn != nil {
		log.Debugf("action: shutdown_connection | result: in_progress | client_id: %v", c.config.ID)

		if tcpConn, ok := c.conn.(*net.TCPConn); ok {
			if err := tcpConn.CloseWrite(); err != nil {
				log.Debugf("action: close_write | result: fail | client_id: %v | error: %v",
					c.config.ID, err)
			} else {
				log.Debugf("action: close_write | result: success | client_id: %v", c.config.ID)
			}
		}

		err := c.conn.Close()
		if err != nil {
			log.Errorf("action: close_connection | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
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

func (c *Client) sendBets(protocol *Protocol) error {
	file, err := os.Open("/agency.csv")
	if err != nil {
		log.Errorf("action: open_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("action: close_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		}
	}()

	reader := csv.NewReader(file)

	batch := make([]Bet, 0, c.config.BatchMaxAmount)
	batchCount := 0
	totalBets := 0

	for c.running {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				// Send last batch if there's any
				if len(batch) > 0 {
					batchCount++
					if err := c.SendBatch(protocol, batch); err != nil {
						log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %d | error: %v",
							c.config.ID, batchCount, err)
						return err
					}
					if err := c.receiveBetResponse(protocol, batchCount, len(batch)); err != nil {
						return err
					}
				}
				break
			}
			log.Errorf("action: read_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}

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

		batch = append(batch, bet)
		totalBets++

		if len(batch) == c.config.BatchMaxAmount {
			batchCount++
			if err := c.SendBatch(protocol, batch); err != nil {
				log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %d | error: %v",
					c.config.ID, batchCount, err)
				return err
			}
			if err := c.receiveBetResponse(protocol, batchCount, len(batch)); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v | total_batches: %d", c.config.ID, batchCount)

	return nil
}

func (c *Client) sendFinishedSending(protocol *Protocol) error {
	message := &FinishedSendingMessage{AgencyID: c.config.ID}
	messageBytes := message.ToBytes()

	err := protocol.SendMessage(c.conn, MsgTypeFinishedSending, messageBytes)
	if err != nil {
		return fmt.Errorf("error sending finished sending: %v", err)
	}

	messageType, responseBytes, err := protocol.ReceiveMessage(c.conn)
	if err != nil {
		return fmt.Errorf("error receiving finished sending response: %v", err)
	}

	if messageType != MsgTypeFinishedSending {
		return fmt.Errorf("unexpected response message type: %d", messageType)
	}

	success, err := BetResponseFromBytes(responseBytes)
	if err != nil {
		return fmt.Errorf("error parsing finished notification response: %v", err)
	}

	if !success {
		return fmt.Errorf("server rejected finished notification")
	}

	log.Infof("action: finished_notification_sent | result: success | client_id: %v", c.config.ID)
	return nil
}

func (c *Client) requestWinners(protocol *Protocol) error {
	for {
		message := &WinnersRequestMessage{AgencyID: c.config.ID}
		messageBytes := message.ToBytes()

		err := protocol.SendMessage(c.conn, MsgTypeWinnersRequest, messageBytes)
		if err != nil {
			return fmt.Errorf("error sending winners request: %v", err)
		}

		messageType, responseBytes, err := protocol.ReceiveMessage(c.conn)
		if err != nil {
			return fmt.Errorf("error receiving winners response: %v", err)
		}

		if messageType == MsgTypeLotteryNotReady {
			log.Infof("action: lottery_not_ready | result: in_progress | client_id: %s", c.config.ID)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if messageType != MsgTypeWinnersResponse {
			return fmt.Errorf("unexpected response message type: %d", messageType)
		}

		winnersResponse, err := WinnersResponseFromBytes(responseBytes)
		if err != nil {
			return fmt.Errorf("error parsing winners response: %v", err)
		}

		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winnersResponse.Winners))
		log.Debug("action: winners_in_response | result: success | ganadores: %v", winnersResponse.Winners)
		return nil
	}
}
