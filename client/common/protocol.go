package common

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/op/go-logging"
)

var protocolLog = logging.MustGetLogger("protocol")

type Protocol struct{}

func NewProtocol() *Protocol {
	return &Protocol{}
}

const HEADER_LEN = 4

func (p *Protocol) SendMessage(conn net.Conn, message interface{}) error {
	jsonData, err := json.Marshal(message)
	if err != nil {
		protocolLog.Errorf("action: serialize_message | result: fail | error: %v", err)
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	header := make([]byte, HEADER_LEN)
	binary.BigEndian.PutUint32(header, uint32(len(jsonData)))

	fullMessage := append(header, jsonData...)

	return p.sendExact(conn, fullMessage)
}

func (p *Protocol) ReceiveMessage(conn net.Conn) (map[string]interface{}, error) {
	header, err := p.receiveExact(conn, HEADER_LEN)
	if err != nil {
		protocolLog.Errorf("action: receive_header | result: fail | error: %v", err)
		return nil, fmt.Errorf("failed to receive header: %w", err)
	}

	messageLength := binary.BigEndian.Uint32(header)

	jsonData, err := p.receiveExact(conn, int(messageLength))
	if err != nil {
		protocolLog.Errorf("action: receive_message | result: fail | error: %v", err)
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}

	var message map[string]interface{}
	if err := json.Unmarshal(jsonData, &message); err != nil {
		protocolLog.Errorf("action: deserialize_message | result: fail | error: %v", err)
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	protocolLog.Debugf("action: receive_message | result: success | message_length: %d", messageLength)
	return message, nil
}

func (p *Protocol) receiveExact(conn net.Conn, numBytes int) ([]byte, error) {
	data := make([]byte, numBytes)
	bytesRead := 0

	for bytesRead < numBytes {
		n, err := conn.Read(data[bytesRead:])
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("connection closed")
			}
			return nil, err
		}
		if n == 0 {
			return nil, fmt.Errorf("connection closed")
		}
		bytesRead += n
	}

	return data, nil
}

func (p *Protocol) sendExact(conn net.Conn, fullMessage []byte) error {
	bytesSent := 0
	for bytesSent < len(fullMessage) {
		n, err := conn.Write(fullMessage[bytesSent:])
		if err != nil {
			protocolLog.Errorf("action: send_message | result: fail | error: %v", err)
			return fmt.Errorf("failed to send message: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("connection broken")
		}
		bytesSent += n
	}
	protocolLog.Debugf("action: send_message | result: success | bytes_sent: %d", bytesSent)
	return nil
}
