#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CEMADEN_URL = os.environ.get("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

TZ = ZoneInfo("America/Sao_Paulo")
TG_MAX = 3500  # margem segura


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/1.2"})
    with urllib.request.urlopen(req, timeout=30) as resp:
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


def chunk_lines(text: str, max_len: int):
    """
    Quebra por linhas, nunca no meio da linha.
    Assim a mensagem fica sempre íntegra e legível.
    """
    lines = (text or "").splitlines()
    buf = ""
    for line in lines:
        # +1 por causa do "\n"
        add = line + "\n"
        if len(buf) + len(add) > max_len:
            if buf.strip():
                yield buf.rstrip("\n")
            buf = add
        else:
            buf += add
    if buf.strip():
        yield buf.rstrip("\n")


def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Saindo sem enviar.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

    for part in chunk_lines(text, TG_MAX):
        payload = json.dumps(
            {
                "chat_id": int(TG_CHAT_ID),
                "text": part,
                "disable_web_page_preview": True,
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                _ = resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
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

    return (
        f"{emoji_nivel(niv)} {uf} {mun}\n"
        f"{tipologia(ev)} | {niv}\n"
        f"Evento: {ev}\n"
        f"IBGE: {codibge} | cod_alerta: {cod_alerta}\n"
        f"Criado: {cri}\n"
        f"Atual.: {atu}"
    )


def main() -> int:
    data = http_get_json(CEMADEN_URL)
    alertas = data.get("alertas", [])
    atualizado = str(data.get("atualizado", "")).strip()

    # somente vigentes
    alertas = [a for a in alertas if a.get("status") == 1]

    state = load_state(STATE_PATH)
    seen = state.get("seen", {})  # cod_alerta -> ult_atualizacao

    novos = []
    atualizados_list = []
    vigentes_agora = {}  # cod_alerta -> ult_atualizacao

    for a in alertas:
        cod = str(a.get("cod_alerta"))
        ult = str(a.get("ult_atualizacao"))
        vigentes_agora[cod] = ult

        if cod not in seen:
            novos.append(a)
        elif seen.get(cod) != ult:
            atualizados_list.append(a)

    encerrados = [cod for cod in seen.keys() if cod not in vigentes_agora]

    def sort_key(a: dict):
        return (-nivel_rank(a.get("nivel", "")), a.get("uf", ""), a.get("municipio", ""))

    novos.sort(key=sort_key)
    atualizados_list.sort(key=sort_key)

    if not novos and not atualizados_list:
        print("Sem novidades.")
    else:
        now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
        parts = []
        parts.append(f"📡 CEMADEN | atualização {now_brt}")
        if atualizado:
            parts.append(f"Conjunto: {atualizado}")

        if novos:
            parts.append("")
            parts.append(f"Novos alertas vigentes ({len(novos)}):")
            for a in novos[:30]:
                parts.append("")
                parts.append(fmt_alert(a))
            if len(novos) > 30:
                parts.append("")
                parts.append(f"... e mais {len(novos) - 30} novos (cortei pra não ficar enorme).")

        if atualizados_list:
            parts.append("")
            parts.append(f"Alertas atualizados ({len(atualizados_list)}):")
            for a in atualizados_list[:30]:
                parts.append("")
                parts.append(fmt_alert(a))
            if len(atualizados_list) > 30:
                parts.append("")
                parts.append(f"... e mais {len(atualizados_list) - 30} atualizados (cortei pra não ficar enorme).")

        if encerrados:
            parts.append("")
            sample = ", ".join(encerrados[:50])
            parts.append(f"Encerrados desde a última checagem ({len(encerrados)}): {sample}")
            if len(encerrados) > 50:
                parts.append(f"... +{len(encerrados) - 50}")

        tg_send("\n".join(parts))

    state["seen"] = vigentes_agora
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
