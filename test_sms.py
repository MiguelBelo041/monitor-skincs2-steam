"""
Envia um SMS de teste usando as mesmas variáveis do .env do projeto.

Uso:
  python test_sms.py
"""

from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv

from notify import send_sms_twilio

load_dotenv()


def main() -> int:
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_n = (os.getenv("TWILIO_FROM_NUMBER") or "").strip()
    to_n = (os.getenv("SMS_TO_NUMBER") or "").strip()

    missing = [
        name
        for name, v in [
            ("TWILIO_ACCOUNT_SID", sid),
            ("TWILIO_AUTH_TOKEN", token),
            ("TWILIO_FROM_NUMBER", from_n),
            ("SMS_TO_NUMBER", to_n),
        ]
        if not v
    ]
    if missing:
        print("Faltam variáveis no .env:", ", ".join(missing), file=sys.stderr)
        return 1

    body = "Teste Steam Monitor — SMS OK."
    print("Enviando SMS de teste...", flush=True)
    ok = send_sms_twilio(
        account_sid=sid,
        auth_token=token,
        from_number=from_n,
        to_number=to_n,
        body=body,
    )
    if ok:
        print("SMS enviado com sucesso.")
        return 0
    print("Falha ao enviar (veja log acima).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
