#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CEMADEN_URL = os.environ.get("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Controles anti-flood / anti-carga
MAX_NEW_ALERTS_PER_RUN = int(os.environ.get("MAX_NEW_ALERTS_PER_RUN", "20"))
SLEEP_BETWEEN_SENDS_SEC = float(os.environ.get("SLEEP_BETWEEN_SENDS_SEC", "2.0"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "30"))

# Quando o Telegram pedir retry_after, tentamos de novo algumas vezes
TG_MAX_RETRIES = int(os.environ.get("TG_MAX_RETRIES", "5"))
TG_EXTRA_BACKOFF_SEC = float(os.environ.get("TG_EXTRA_BACKOFF_SEC", "1.0"))

SEND_SUMMARY_WHEN_CAPPED = os.environ.get("SEND_SUMMARY_WHEN_CAPPED", "1").strip() == "1"

TZ = ZoneInfo("America/Sao_Paulo")


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/2.2"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"seen": {}, "last_run": None}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _tg_send_once(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": int(TG_CHAT_ID),
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        _ = resp.read()


def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Saindo sem enviar.")
        return

    attempt = 0
    backoff = 0.0

    while True:
        try:
            if backoff > 0:
                time.sleep(backoff)
            _tg_send_once(text)
            return

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            # tenta parsear o JSON de erro do Telegram
            retry_after = None
            try:
                j = json.loads(body)
                retry_after = (j.get("parameters") or {}).get("retry_after")
            except Exception:
                pass

            if e.code == 429 and retry_after is not None:
                attempt += 1
                if attempt > TG_MAX_RETRIES:
                    print("Telegram 429 excedeu tentativas. Último body:", body)
                    raise

                wait_s = float(retry_after) + TG_EXTRA_BACKOFF_SEC
                print(f"Telegram 429, esperando {wait_s:.1f}s e tentando de novo (tentativa {attempt}/{TG_MAX_RETRIES})")
                backoff = wait_s
                continue

            print("Telegram HTTPError:", e.code, body)
            raise


def nivel_rank(nivel: str) -> int:
    n = (nivel or "").strip().lower()
    if n == "muito alto":
        return 3
    if n == "alto":
        return 2
    if n == "moderado":
        return 1
    return 0


def emoji_nivel(nivel: str) -> str:
    n = (nivel or "").strip().lower()
    if n == "muito alto":
        return "🟥"
    if n == "alto":
        return "🟧"
    if n == "moderado":
        return "🟨"
    return "⬜"


def tipologia(evento: str) -> str:
    e = (evento or "").lower()
    if "mov" in e:
        return "Mov. de Massa"
    if "hidrol" in e:
        return "Hidrológico"
    return "Outro"


def fmt_alert(a: dict) -> str:
    uf = str(a.get("uf", "")).strip()
    mun = str(a.get("municipio", "")).strip()
    ev = str(a.get("evento", "")).strip()
    niv = str(a.get("nivel", "")).strip()
    cri = str(a.get("datahoracriacao", "")).strip()
    atu = str(a.get("ult_atualizacao", "")).strip()
    codibge = a.get("codibge", "")
    cod_alerta = a.get("cod_alerta", "")
    lat = a.get("latitude", "")
    lon = a.get("longitude", "")

    lines = [
        "📣 CEMADEN",
        f"{emoji_nivel(niv)} {uf} {mun}",
        f"{tipologia(ev)} | {niv}",
        f"Evento: {ev}",
        f"IBGE: {codibge} | cod_alerta: {cod_alerta}",
        f"Criado: {cri}",
        f"Atual.: {atu}",
    ]
    if lat != "" and lon != "":
        lines.append(f"Coord: {lat}, {lon}")
    return "\n".join(lines)


def main() -> int:
    state = load_state(STATE_PATH)
    seen = state.get("seen", {})  # cod_alerta -> ult_atualizacao

    data = http_get_json(CEMADEN_URL)

    alertas = data.get("alertas", [])
    alertas = [a for a in alertas if a.get("status") == 1]  # só vigentes

    novos = []
    for a in alertas:
        cod = str(a.get("cod_alerta"))
        if cod and cod not in seen:
            novos.append(a)

    novos.sort(key=lambda a: (-nivel_rank(a.get("nivel", "")), a.get("uf", ""), a.get("municipio", "")))

    if not novos:
        print("Sem novos alertas.")
    else:
        total = len(novos)
        enviar = novos[:MAX_NEW_ALERTS_PER_RUN]

        for idx, a in enumerate(enviar, start=1):
            tg_send(fmt_alert(a))
            if idx < len(enviar):
                time.sleep(SLEEP_BETWEEN_SENDS_SEC)

        if total > len(enviar) and SEND_SUMMARY_WHEN_CAPPED:
            now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
            tg_send(
                "⚠️ CEMADEN\n"
                f"Foram detectados {total} alertas novos, mas enviei só {len(enviar)} nesta rodada.\n"
                f"Horário: {now_brt}\n"
                "Os demais serão enviados nas próximas execuções."
            )

    # marca todos os vigentes como vistos
    for a in alertas:
        cod = str(a.get("cod_alerta"))
        ult = str(a.get("ult_atualizacao"))
        if cod:
            seen[cod] = ult

    state["seen"] = seen
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
