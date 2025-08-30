from typing import Any, Dict

from server.common.utils import Bet

FIELDS = ["AGENCY_ID", "NOMBRE", "APELLIDO", "DOCUMENTO", "NACIMIENTO", "NUMERO"]


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

class BetResponseMessage:
    @staticmethod
    def to_bytes(success: bool) -> bytes:
        if success:
            return SUCCESS_STR.encode("utf-8")
        return ERROR_STR.encode("utf-8")
