from typing import Any, Dict
import logging
from .utils import Bet

FIELDS = ["AGENCY_ID", "NOMBRE", "APELLIDO", "DOCUMENTO", "NACIMIENTO", "NUMERO"]

MSG_TYPE_BET = 0x01
MSG_TYPE_FINISHED_SENDING = 0x02
MSG_TYPE_WINNERS_REQUEST = 0x03
MSG_TYPE_WINNERS_RESPONSE = 0x04
MSG_TYPE_LOTTERY_NOT_READY = 0x05


class BetMessage:
    # El formato de un mensaje Bet va a ser siempre un string de la forma: "AGENCY_ID=...,NOMBRE=...,APELLIDO=...,DOCUMENTO=...,NACIMIENTO=...,NUMERO=..."
    # Todos los campos van a ser strings que vienen en bytes (utf8)
    @staticmethod
    def from_bytes(b: bytes) -> "Bet":
        s = b.decode("utf-8")

        parts = [p.strip() for p in s.split(",") if p.strip() != ""]
        kv: Dict[str, str] = {}
        for p in parts:
            if "=" not in p:
                continue
            k, v = p.split("=", 1)
            k = k.strip().upper()
            v = v.strip()
            kv[k] = v

        for req in FIELDS:
            if req not in kv:
                raise ValueError(f"missing field {req}")

        logging.debug("action: bet_parsed | result: success | bet: %s", kv)
        bet = Bet(
            kv["AGENCY_ID"],
            kv["NOMBRE"],
            kv["APELLIDO"],
            kv["DOCUMENTO"],
            kv["NACIMIENTO"],
            kv["NUMERO"],
        )
        return bet


SUCCESS_STR = "success"
ERROR_STR = "error"


def parse_agency_id(b: bytes) -> str:
    s = b.decode("utf-8")

    if "=" not in s:
        raise ValueError("invalid agency ID message format")

    key, value = s.split("=", 1)
    if key.strip().upper() != "AGENCY_ID":
        raise ValueError("expected AGENCY_ID field")

    return value.strip()


class BetResponseMessage:
    @staticmethod
    def to_bytes(success: bool) -> bytes:
        if success:
            return SUCCESS_STR.encode("utf-8")
        return ERROR_STR.encode("utf-8")


class FinishedSendingMessage:
    @staticmethod
    def from_bytes(b: bytes) -> str:
        return parse_agency_id(b)


class WinnersRequestMessage:
    @staticmethod
    def from_bytes(b: bytes) -> str:
        return parse_agency_id(b)


class WinnersResponseMessage:
    @staticmethod
    def to_bytes(winners: list[str]) -> bytes:
        winners_str = ",".join(winners)
        return f"WINNERS={winners_str}".encode("utf-8")
