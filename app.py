"""
Servidor web + worker que consulta a Steam a cada N segundos e dispara alertas.
Execute: python app.py  (depois abra http://127.0.0.1:5050)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from notify import send_email_smtp, send_sms_twilio
from steam_monitor import PriceOverview, build_priceoverview_url, fetch_price_overview

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

HISTORY_MAX = 200


def _timezone_brasilia():
    """America/Sao_Paulo via tzdata; se indisponível (ex.: Windows sem tzdata), UTC−3."""
    try:
        return ZoneInfo("America/Sao_Paulo")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def format_timestamp_brasilia() -> str:
    """Data/hora para exibição em horário de Brasília."""
    return datetime.now(_timezone_brasilia()).strftime("%d/%m/%Y %H:%M:%S · Brasília")


@dataclass
class CheckRecord:
    at: str
    median_brl: float | None
    lowest_brl: float | None
    volume: str | None
    success: bool
    error: str | None
    alert_triggered: bool


@dataclass
class AppState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_ok: PriceOverview | None = None
    last_error: str | None = None
    last_check_at: str | None = None
    history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_MAX))
    # Evita spam: alerta quando cruza para >= limite (subiu de abaixo para acima)
    was_alert_active: bool = False


state = AppState()


def getenv_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    return float(v.replace(",", "."))


def getenv_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    return int(v)


def send_notifications(subject: str, body: str) -> None:
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    if smtp_host:
        ok = send_email_smtp(
            host=smtp_host,
            port=getenv_int("SMTP_PORT", 587),
            user=os.getenv("SMTP_USER", ""),
            password=os.getenv("SMTP_PASSWORD", ""),
            mail_from=os.getenv("EMAIL_FROM", ""),
            mail_to=os.getenv("EMAIL_TO", ""),
            subject=subject,
            body=body,
        )
        logger.info("Email enviado: %s", ok)

    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    if sid:
        ok = send_sms_twilio(
            account_sid=sid,
            auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
            from_number=os.getenv("TWILIO_FROM_NUMBER", ""),
            to_number=os.getenv("SMS_TO_NUMBER", ""),
            body=f"{subject}\n{body}",
        )
        logger.info("SMS enviado: %s", ok)


def get_database_url() -> str | None:
    v = (os.getenv("DATABASE_URL") or "").strip()
    return v or None


def db_connect():
    try:
        import psycopg2
    except ImportError as e:
        raise RuntimeError("psycopg2-binary nao instalado. Rode: python -m pip install -r requirements.txt") from e

    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL nao definido no .env")
    return psycopg2.connect(url)


def ensure_schema() -> None:
    # Garante as tabelas para multi-alertas (idempotente)
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.alerts (
                  id BIGSERIAL PRIMARY KEY,
                  market_hash_name TEXT NOT NULL,
                  currency INTEGER NOT NULL DEFAULT 7,
                  threshold_brl NUMERIC(12,2) NOT NULL,
                  poll_interval_seconds INTEGER NOT NULL DEFAULT 600,
                  notify_email BOOLEAN NOT NULL DEFAULT FALSE,
                  notify_sms BOOLEAN NOT NULL DEFAULT TRUE,
                  enabled BOOLEAN NOT NULL DEFAULT TRUE,
                  last_median_brl NUMERIC(12,2),
                  last_alert_active BOOLEAN NOT NULL DEFAULT FALSE,
                  last_checked_at TIMESTAMPTZ,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS alerts_enabled_idx
                  ON public.alerts (enabled);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS alerts_mhn_idx
                  ON public.alerts (market_hash_name);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.alert_checks (
                  id BIGSERIAL PRIMARY KEY,
                  alert_id BIGINT NOT NULL REFERENCES public.alerts(id) ON DELETE CASCADE,
                  checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  success BOOLEAN NOT NULL,
                  median_brl NUMERIC(12,2),
                  lowest_brl NUMERIC(12,2),
                  volume TEXT,
                  error TEXT,
                  alert_triggered BOOLEAN NOT NULL DEFAULT FALSE
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS alert_checks_alert_id_idx
                  ON public.alert_checks (alert_id, checked_at DESC);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.notifications (
                  id BIGSERIAL PRIMARY KEY,
                  alert_id BIGINT NOT NULL REFERENCES public.alerts(id) ON DELETE CASCADE,
                  sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  channel TEXT NOT NULL,
                  ok BOOLEAN NOT NULL,
                  message TEXT
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.sync_state (
                  name TEXT PRIMARY KEY,
                  last_start_offset INTEGER NOT NULL DEFAULT 0,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


def db_fetch_items(q: str, limit: int = 20) -> list[dict[str, Any]]:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT market_hash_name, display_name, sell_price_text, sell_listings
                FROM public.cs2_market_items
                WHERE display_name ILIKE %s OR market_hash_name ILIKE %s
                ORDER BY sell_listings DESC NULLS LAST
                LIMIT %s;
                """,
                (f"%{q}%", f"%{q}%", limit),
            )
            rows = cur.fetchall()
            return [
                {
                    "market_hash_name": r[0],
                    "display_name": r[1],
                    "sell_price_text": r[2],
                    "sell_listings": r[3],
                }
                for r in rows
            ]
    finally:
        conn.close()


def db_list_alerts() -> list[dict[str, Any]]:
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, market_hash_name, currency, threshold_brl, poll_interval_seconds,
                       notify_email, notify_sms, enabled, last_median_brl, last_alert_active,
                       last_checked_at, created_at, updated_at
                FROM public.alerts
                ORDER BY id DESC;
                """
            )
            rows = cur.fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "market_hash_name": r[1],
                        "currency": r[2],
                        "threshold_brl": float(r[3]),
                        "poll_interval_seconds": r[4],
                        "notify_email": bool(r[5]),
                        "notify_sms": bool(r[6]),
                        "enabled": bool(r[7]),
                        "last_median_brl": float(r[8]) if r[8] is not None else None,
                        "last_alert_active": bool(r[9]),
                        "last_checked_at": r[10].isoformat() if r[10] else None,
                        "created_at": r[11].isoformat() if r[11] else None,
                        "updated_at": r[12].isoformat() if r[12] else None,
                    }
                )
            return out
    finally:
        conn.close()


def db_create_alert(
    market_hash_name: str,
    threshold_brl: float,
    poll_interval_seconds: int,
    notify_email: bool,
    notify_sms: bool,
    currency: int = 7,
) -> int:
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.alerts
                  (market_hash_name, currency, threshold_brl, poll_interval_seconds, notify_email, notify_sms, enabled)
                VALUES
                  (%s, %s, %s, %s, %s, %s, TRUE)
                RETURNING id;
                """,
                (market_hash_name, currency, threshold_brl, poll_interval_seconds, notify_email, notify_sms),
            )
            alert_id = int(cur.fetchone()[0])
        conn.commit()
        return alert_id
    finally:
        conn.close()


def db_set_alert_enabled(alert_id: int, enabled: bool) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.alerts SET enabled=%s, updated_at=NOW() WHERE id=%s;",
                (enabled, alert_id),
            )
        conn.commit()
    finally:
        conn.close()


def db_delete_alert(alert_id: int) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.alerts WHERE id=%s;", (alert_id,))
        conn.commit()
    finally:
        conn.close()


def db_due_alerts(now_ts: float) -> list[dict[str, Any]]:
    # Retorna alertas enabled que estao "vencidos" (ultima checagem + intervalo <= agora)
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, market_hash_name, currency, threshold_brl, poll_interval_seconds,
                       notify_email, notify_sms, last_alert_active
                FROM public.alerts
                WHERE enabled = TRUE
                  AND (
                    last_checked_at IS NULL OR
                    EXTRACT(EPOCH FROM (NOW() - last_checked_at)) >= poll_interval_seconds
                  )
                ORDER BY COALESCE(last_checked_at, '1970-01-01'::timestamptz) ASC
                LIMIT 200;
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "id": int(r[0]),
                    "market_hash_name": r[1],
                    "currency": int(r[2]),
                    "threshold_brl": float(r[3]),
                    "poll_interval_seconds": int(r[4]),
                    "notify_email": bool(r[5]),
                    "notify_sms": bool(r[6]),
                    "last_alert_active": bool(r[7]),
                }
                for r in rows
            ]
    finally:
        conn.close()


def db_write_check(
    alert_id: int,
    success: bool,
    median_brl: float | None,
    lowest_brl: float | None,
    volume: str | None,
    error: str | None,
    alert_triggered: bool,
    last_alert_active: bool | None,
) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.alert_checks
                  (alert_id, success, median_brl, lowest_brl, volume, error, alert_triggered)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s);
                """,
                (alert_id, success, median_brl, lowest_brl, volume, error, alert_triggered),
            )
            cur.execute(
                """
                UPDATE public.alerts
                SET last_checked_at = NOW(),
                    last_median_brl = %s,
                    last_alert_active = COALESCE(%s, last_alert_active),
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (median_brl, last_alert_active, alert_id),
            )
        conn.commit()
    finally:
        conn.close()


def db_log_notification(alert_id: int, channel: str, ok: bool, message: str) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.notifications (alert_id, channel, ok, message) VALUES (%s, %s, %s, %s);",
                (alert_id, channel, ok, message),
            )
        conn.commit()
    finally:
        conn.close()


def run_one_check(
    url: str,
    threshold: float,
) -> CheckRecord:
    now = format_timestamp_brasilia()
    alert_triggered = False
    error_msg: str | None = None

    try:
        overview = fetch_price_overview(url)
        median = overview.median_brl

        with state.lock:
            state.last_ok = overview
            state.last_error = None
            state.last_check_at = now

            above = median is not None and median >= threshold
            # Alerta na transição: estava abaixo do critério e agora está acima
            if above and not state.was_alert_active:
                alert_triggered = True
                state.was_alert_active = True
            elif median is not None and median < threshold:
                state.was_alert_active = False

            rec = CheckRecord(
                at=now,
                median_brl=median,
                lowest_brl=overview.lowest_brl,
                volume=overview.volume,
                success=overview.success,
                error=None,
                alert_triggered=alert_triggered,
            )
            state.history.appendleft(rec)

        if alert_triggered:
            body = (
                f"Mediana atual: R$ {median:.2f}\n"
                f"Mínimo listado: {overview.lowest_price_raw}\n"
                f"Volume: {overview.volume}\n"
                f"Limite configurado: R$ {threshold:.2f}"
            )
            send_notifications(
                "[Steam Market] Mediana >= limite",
                body,
            )

        return rec

    except Exception as e:
        error_msg = str(e)
        logger.exception("Erro na consulta: %s", e)
        with state.lock:
            state.last_error = error_msg
            state.last_check_at = now
            rec = CheckRecord(
                at=now,
                median_brl=None,
                lowest_brl=None,
                volume=None,
                success=False,
                error=error_msg,
                alert_triggered=False,
            )
            state.history.appendleft(rec)
        return rec


def worker_loop(stop_event: threading.Event, url: str, interval: float, threshold: float) -> None:
    while not stop_event.is_set():
        run_one_check(url, threshold)
        stop_event.wait(timeout=interval)


def worker_loop_multi(stop_event: threading.Event) -> None:
    # Loop que processa alertas (DB) e faz dedupe por item
    rate_limit_seconds = float(os.getenv("STEAM_RATE_LIMIT_SECONDS", "1.5"))
    appid = int(os.getenv("STEAM_APPID", "730"))
    while not stop_event.is_set():
        try:
            due = db_due_alerts(time.time())
        except Exception as e:
            logger.exception("Falha ao ler alertas: %s", e)
            stop_event.wait(timeout=10)
            continue

        if not due:
            stop_event.wait(timeout=3)
            continue

        # Dedupe por item+currency
        grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for a in due:
            key = (a["market_hash_name"], a["currency"])
            grouped.setdefault(key, []).append(a)

        for (mhn, currency), alerts in grouped.items():
            url = build_priceoverview_url(appid=appid, currency=currency, market_hash_name=mhn)
            try:
                overview = fetch_price_overview(url)
                median = overview.median_brl
                lowest = overview.lowest_brl
                vol = overview.volume
                for a in alerts:
                    threshold = float(a["threshold_brl"])
                    above = median is not None and median >= threshold
                    triggered = bool(above and not a["last_alert_active"])
                    new_active = bool(above) if median is not None else None

                    db_write_check(
                        alert_id=a["id"],
                        success=bool(overview.success),
                        median_brl=median,
                        lowest_brl=lowest,
                        volume=vol,
                        error=None,
                        alert_triggered=triggered,
                        last_alert_active=new_active,
                    )

                    if triggered:
                        subject = "[Steam Market] Preco medio >= limite"
                        body = (
                            f"Item: {mhn}\n"
                            f"Preco medio: R$ {median:.2f}\n"
                            f"Menor preco anunciado: {overview.lowest_price_raw}\n"
                            f"Ultimas vendas: {overview.volume}\n"
                            f"Limite: R$ {threshold:.2f}"
                        )

                        # canais por alerta
                        if a["notify_email"]:
                            smtp_host = (os.getenv("SMTP_HOST") or "").strip()
                            if smtp_host:
                                ok = send_email_smtp(
                                    host=smtp_host,
                                    port=getenv_int("SMTP_PORT", 587),
                                    user=os.getenv("SMTP_USER", ""),
                                    password=os.getenv("SMTP_PASSWORD", ""),
                                    mail_from=os.getenv("EMAIL_FROM", ""),
                                    mail_to=os.getenv("EMAIL_TO", ""),
                                    subject=subject,
                                    body=body,
                                )
                                db_log_notification(a["id"], "email", bool(ok), body)

                        if a["notify_sms"]:
                            sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
                            if sid:
                                ok = send_sms_twilio(
                                    account_sid=sid,
                                    auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
                                    from_number=os.getenv("TWILIO_FROM_NUMBER", ""),
                                    to_number=os.getenv("SMS_TO_NUMBER", ""),
                                    body=f"{subject}\n{mhn}\nR$ {median:.2f}",
                                )
                                db_log_notification(a["id"], "sms", bool(ok), body)

            except Exception as e:
                err = str(e)
                logger.exception("Erro priceoverview %s: %s", mhn, e)
                for a in alerts:
                    db_write_check(
                        alert_id=a["id"],
                        success=False,
                        median_brl=None,
                        lowest_brl=None,
                        volume=None,
                        error=err,
                        alert_triggered=False,
                        last_alert_active=None,
                    )

            # rate limit entre itens
            stop_event.wait(timeout=rate_limit_seconds)


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

    @app.after_request
    def add_no_cache_headers(response):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    @app.route("/api/status")
    def api_status():
        threshold = getenv_float("ALERT_MEDIAN_BRL", 920)
        poll_sec = getenv_int("POLL_INTERVAL_SECONDS", 600)
        url = os.getenv(
            "STEAM_PRICE_URL",
            "https://steamcommunity.com/market/priceoverview/?appid=730&currency=7&market_hash_name="
            "%E2%98%85%20Driver%20Gloves%20|%20Seigaiha%20(Field-Tested)",
        )

        with state.lock:
            last = state.last_ok
            hist = list(state.history)
            last_check_at = state.last_check_at
            last_error = state.last_error
            was_alert = state.was_alert_active

        payload: dict[str, Any] = {
            "threshold_brl": threshold,
            "poll_interval_seconds": poll_sec,
            "steam_url": url,
            "last_check_at": last_check_at,
            "last_error": last_error,
            "was_alert_active": was_alert,
            "last_success": None,
            "history": [asdict(x) for x in hist[:50]],
        }

        if last:
            payload["last_success"] = {
                "median_brl": last.median_brl,
                "lowest_brl": last.lowest_brl,
                "median_price_raw": last.median_price_raw,
                "lowest_price_raw": last.lowest_price_raw,
                "volume": last.volume,
                "success": last.success,
            }

        return jsonify(payload)

    @app.route("/alerts")
    def alerts_page():
        return render_template("alerts.html")

    @app.route("/api/items")
    def api_items():
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"items": []})
        return jsonify({"items": db_fetch_items(q, limit=20)})

    @app.route("/api/alerts")
    def api_alerts_list():
        return jsonify({"alerts": db_list_alerts()})

    @app.route("/api/alerts", methods=["POST"])
    def api_alerts_create():
        data = request.get_json(force=True, silent=True) or {}
        mhn = (data.get("market_hash_name") or "").strip()
        if not mhn:
            return jsonify({"error": "market_hash_name obrigatorio"}), 400
        threshold = float(data.get("threshold_brl") or 0)
        if threshold <= 0:
            return jsonify({"error": "threshold_brl invalido"}), 400
        poll = int(data.get("poll_interval_seconds") or 600)
        poll = max(60, poll)
        notify_email = bool(data.get("notify_email") or False)
        notify_sms = bool(data.get("notify_sms") if "notify_sms" in data else True)
        currency = int(data.get("currency") or 7)
        alert_id = db_create_alert(mhn, threshold, poll, notify_email, notify_sms, currency=currency)
        return jsonify({"id": alert_id})

    @app.route("/api/alerts/<int:alert_id>/enable", methods=["POST"])
    def api_alerts_enable(alert_id: int):
        data = request.get_json(force=True, silent=True) or {}
        enabled = bool(data.get("enabled"))
        db_set_alert_enabled(alert_id, enabled)
        return jsonify({"ok": True})

    @app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
    def api_alerts_delete(alert_id: int):
        db_delete_alert(alert_id)
        return jsonify({"ok": True})

    return app


def main() -> None:
    url = os.getenv(
        "STEAM_PRICE_URL",
        "https://steamcommunity.com/market/priceoverview/?appid=730&currency=7&market_hash_name="
        "%E2%98%85%20Driver%20Gloves%20|%20Seigaiha%20(Field-Tested)",
    )
    interval = getenv_int("POLL_INTERVAL_SECONDS", 600)
    threshold = getenv_float("ALERT_MEDIAN_BRL", 920)

    logger.info("Limite alerta mediana: R$ %.2f | intervalo: %ds | URL definida em STEAM_PRICE_URL", threshold, interval)

    ensure_schema()

    stop_event = threading.Event()

    # Worker antigo (single item) continua ativo para o dashboard /api/status
    t1 = threading.Thread(
        target=worker_loop,
        args=(stop_event, url, float(interval), threshold),
        daemon=True,
        name="steam-poller-single",
    )
    t1.start()

    # Worker novo (multi alertas via Postgres)
    t2 = threading.Thread(
        target=worker_loop_multi,
        args=(stop_event,),
        daemon=True,
        name="steam-poller-multi",
    )
    t2.start()

    app = create_app()
    try:
        app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5050")), debug=False, use_reloader=False)
    except KeyboardInterrupt:
        stop_event.set()


if __name__ == "__main__":
    main()
