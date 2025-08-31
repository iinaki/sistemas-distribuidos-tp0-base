package common

import (
	"fmt"
	"strings"
)

const (
	SUCCESS_STR = "success"
	ERROR_STR   = "error"
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
