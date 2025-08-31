package common

import (
	"fmt"
	"strings"
)

const (
	SUCCESS_STR = "success"
	ERROR_STR   = "error"
)

func CreateBetMessage(agencyID, nombre, apellido, documento, nacimiento, numero string) []byte {
	message := fmt.Sprintf("AGENCY_ID=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s",
		agencyID, nombre, apellido, documento, nacimiento, numero)
	return []byte(message)
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
