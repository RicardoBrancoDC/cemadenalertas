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

MAX_NEW_ALERTS_PER_RUN = int(os.environ.get("MAX_NEW_ALERTS_PER_RUN", "500"))
SLEEP_BETWEEN_SENDS_SEC = float(os.environ.get("SLEEP_BETWEEN_SENDS_SEC", "1.2"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "30"))

TG_MAX_RETRIES = int(os.environ.get("TG_MAX_RETRIES", "6"))
TG_EXTRA_BACKOFF_SEC = float(os.environ.get("TG_EXTRA_BACKOFF_SEC", "1.0"))

SEND_SUMMARY_WHEN_CAPPED = os.environ.get("SEND_SUMMARY_WHEN_CAPPED", "1").strip() == "1"

TZ = ZoneInfo("America/Sao_Paulo")
TG_TEXT_LIMIT = 3800


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/3.2"})
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
            "parse_mode": "Markdown",
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
                print(f"Telegram 429, esperando {wait_s:.1f}s e tentando de novo ({attempt}/{TG_MAX_RETRIES})")
                backoff = wait_s
                continue

            print("Telegram HTTPError:", e.code, body)
            raise


def chunks_by_lines(text: str, limit: int):
    lines = (text or "").splitlines()
    buf = ""
    for line in lines:
        add = line + "\n"
        if len(buf) + len(add) > limit:
            if buf.strip():
                yield buf.rstrip("\n")
            buf = add
        else:
            buf += add
    if buf.strip():
        yield buf.rstrip("\n")


def esc_md(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    for ch in ["_", "*", "`", "["]:
        s = s.replace(ch, "\\" + ch)
    return s


def nivel_rank(nivel: str) -> int:
    n = (nivel or "").strip().lower()
    if n == "muito alto":
        return 3
    if n == "alto":
        return 2
    if n == "moderado":
        return 1
    return 0


def nivel_key(nivel: str) -> str:
    n = (nivel or "").strip().lower()
    if n == "muito alto":
        return "MUITO ALTO"
    if n == "alto":
        return "ALTO"
    if n == "moderado":
        return "MODERADO"
    return "OUTRO"


def nivel_chip(nivel_key_str: str) -> str:
    # você pediu: Moderado laranja, Alto vermelho, Muito Alto roxo
    if nivel_key_str == "MODERADO":
        return "🟧 *MODERADO*"
    if nivel_key_str == "ALTO":
        return "🟥 *ALTO*"
    if nivel_key_str == "MUITO ALTO":
        return "🟪 *MUITO ALTO*"
    return "⬜ *OUTRO*"


def tipologia(evento: str) -> str:
    e = (evento or "").lower()
    if "hidrol" in e:
        return "Hidrológico"
    if "mov" in e:
        return "Movimento de Massa"
    return "Outro"


def tipologia_icon(tip: str) -> str:
    if tip == "Hidrológico":
        return "💧"
    if tip == "Movimento de Massa":
        return "⛰️"
    return "❓"


def fmt_dt(dt_str: str) -> str:
    s = (dt_str or "").strip()
    if not s:
        return ""
    try:
        base = s.split(".")[0]
        d, t = base.split(" ")
        _yyyy, mm, dd = d.split("-")
        hh, mi, _ss = t.split(":")
        return f"{dd}/{mm} {hh}:{mi}"
    except Exception:
        return s


def item_line(a: dict) -> str:
    mun = esc_md(str(a.get("municipio", "")).strip())
    cod = esc_md(str(a.get("cod_alerta", "")).strip())
    dt = esc_md(fmt_dt(str(a.get("datahoracriacao", "")).strip()))
    return f"{mun} (Cód: {cod} - Data:{dt})"


def build_messages_by_uf(alertas_novos: list[dict]) -> list[str]:
    # uf -> tipologia -> nivel -> itens
    grouped: dict[str, dict[str, dict[str, list[dict]]]] = {}

    for a in alertas_novos:
        uf = str(a.get("uf", "")).strip() or "??"
        tip = tipologia(str(a.get("evento", "")))
        niv = nivel_key(str(a.get("nivel", "")))
        grouped.setdefault(uf, {}).setdefault(tip, {}).setdefault(niv, []).append(a)

    tip_order = ["Hidrológico", "Movimento de Massa", "Outro"]
    niv_order = ["MUITO ALTO", "ALTO", "MODERADO", "OUTRO"]

    messages: list[str] = []

    for uf in sorted(grouped.keys()):
        uf_block = grouped[uf]

        # resumo por nível
        cnt_lvl = {"MUITO ALTO": 0, "ALTO": 0, "MODERADO": 0, "OUTRO": 0}
        # resumo por tipo
        cnt_tip = {"Hidrológico": 0, "Movimento de Massa": 0, "Outro": 0}

        for tip, tip_block in uf_block.items():
            for niv, items in tip_block.items():
                cnt_lvl[niv] = cnt_lvl.get(niv, 0) + len(items)
                cnt_tip[tip] = cnt_tip.get(tip, 0) + len(items)

        resumo_linhas = [
            f"🟪 {cnt_lvl['MUITO ALTO']}  🟥 {cnt_lvl['ALTO']}  🟧 {cnt_lvl['MODERADO']}",
            f"💧 {cnt_tip.get('Hidrológico', 0)}  ⛰️ {cnt_tip.get('Movimento de Massa', 0)}",
        ]

        lines = [
            f"📣 *Alertas {esc_md(uf)}* (novos)",
            *resumo_linhas,
            "",
        ]

        for tip in tip_order:
            if tip not in uf_block:
                continue

            lines.append(f"*{tipologia_icon(tip)} {esc_md(tip)}:*")
            tip_block = uf_block[tip]

            for niv in niv_order:
                items = tip_block.get(niv, [])
                if not items:
                    continue

                items.sort(key=lambda a: (str(a.get("municipio", "")), -nivel_rank(a.get("nivel", ""))))
                joined = "; ".join(item_line(a) for a in items)
                lines.append(f"{nivel_chip(niv)}: {joined}")

            lines.append("")

        text = "\n".join(lines).strip()

        for part in chunks_by_lines(text, TG_TEXT_LIMIT):
            messages.append(part)

    return messages


def main() -> int:
    state = load_state(STATE_PATH)
    seen = state.get("seen", {})

    data = http_get_json(CEMADEN_URL)

    alertas = data.get("alertas", [])
    alertas = [a for a in alertas if a.get("status") == 1]

    novos = []
    for a in alertas:
        cod = str(a.get("cod_alerta"))
        if cod and cod not in seen:
            novos.append(a)

    total = len(novos)
    enviar = novos[:MAX_NEW_ALERTS_PER_RUN]

    if not enviar:
        print("Sem novos alertas.")
    else:
        msgs = build_messages_by_uf(enviar)
        for i, msg in enumerate(msgs, start=1):
            tg_send(msg)
            if i < len(msgs):
                time.sleep(SLEEP_BETWEEN_SENDS_SEC)

        if total > len(enviar) and SEND_SUMMARY_WHEN_CAPPED:
            now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
            tg_send(
                "⚠️ *CEMADEN*\n"
                f"Foram detectados {total} alertas novos, mas processei só {len(enviar)} nesta rodada.\n"
                f"Horário: {esc_md(now_brt)}\n"
                "Os demais entram nas próximas execuções."
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
