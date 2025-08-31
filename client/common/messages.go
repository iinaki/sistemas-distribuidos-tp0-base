package common

import (
	"fmt"
	"strings"
)

const (
	SUCCESS_STR = "success"
	ERROR_STR   = "error"
)

const (
	MsgTypeBet             = 0x01
	MsgTypeFinishedSending = 0x02
	MsgTypeWinnersRequest  = 0x03
	MsgTypeWinnersResponse = 0x04
	MsgTypeLotteryNotReady = 0x05
)

type Bet struct {
	AgencyID   string
	Nombre     string
	Apellido   string
	Documento  string
	Nacimiento string
	Numero     string
}

func (b *Bet) toBytes() []byte {
	return []byte(fmt.Sprintf("AGENCY_ID=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s",
		b.AgencyID, b.Nombre, b.Apellido, b.Documento, b.Nacimiento, b.Numero))
}

func ParseBetResponse(responseBytes []byte) (bool, error) {
	response := strings.TrimSpace(string(responseBytes))

	switch response {
	case SUCCESS_STR:
		return true, nil
	case ERROR_STR:
		return false, nil
	default:
		return false, fmt.Errorf("unknown response: %s", response)
	}
}

type FinishedSendingMessage struct {
	AgencyID string
}

func (m *FinishedSendingMessage) ToBytes() []byte {
	return []byte(fmt.Sprintf("AGENCY_ID=%s", m.AgencyID))
}

type WinnersRequestMessage struct {
	AgencyID string
}

func (m *WinnersRequestMessage) ToBytes() []byte {
	return []byte(fmt.Sprintf("AGENCY_ID=%s", m.AgencyID))
}

type WinnersResponseMessage struct {
	Winners []string
}

// El mensaje winners va ser de la forma "WINNERS=12345678,87654321,11223344"
func WinnersResponseFromBytes(responseBytes []byte) (*WinnersResponseMessage, error) {
	response := strings.TrimSpace(string(responseBytes))

	if !strings.Contains(response, "=") {
		return nil, fmt.Errorf("invalid winners response format")
	}

	parts := strings.SplitN(response, "=", 2)
	if strings.TrimSpace(strings.ToUpper(parts[0])) != "WINNERS" {
		return nil, fmt.Errorf("expected WINNERS field")
	}

	winnersStr := strings.TrimSpace(parts[1])
	if winnersStr == "" {
		return &WinnersResponseMessage{Winners: []string{}}, nil
	}

	winners := make([]string, 0)
	for _, dni := range strings.Split(winnersStr, ",") {
		winners = append(winners, strings.TrimSpace(dni))
	}

	return &WinnersResponseMessage{Winners: winners}, nil
}
