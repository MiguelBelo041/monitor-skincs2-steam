"""Envio opcional de email (SMTP) e SMS (Twilio)."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send_email_smtp(
    host: str,
    port: int,
    user: str,
    password: str,
    mail_from: str,
    mail_to: str,
    subject: str,
    body: str,
) -> bool:
    msg = MIMEMultipart()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(mail_from, mail_to.split(","), msg.as_string())
        return True
    except Exception as e:
        logger.exception("Falha ao enviar email: %s", e)
        return False


def send_sms_twilio(
    account_sid: str,
    auth_token: str,
    from_number: str,
    to_number: str,
    body: str,
) -> bool:
    try:
        from twilio.rest import Client
    except ImportError:
        logger.error("Twilio não instalado. Use: pip install twilio")
        return False

    try:
        client = Client(account_sid, auth_token)
        client.messages.create(body=body, from_=from_number, to=to_number)
        return True
    except Exception as e:
        logger.exception("Falha ao enviar SMS: %s", e)
        return False
