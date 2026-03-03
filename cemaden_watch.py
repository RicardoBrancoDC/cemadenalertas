#!/usr/bin/env python3
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CEMADEN_URL = os.environ.get("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

TZ = ZoneInfo("America/Sao_Paulo")

def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/1.0"})
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

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Saindo sem enviar.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        _ = resp.read()

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
    # datas vêm como "YYYY-MM-DD HH:MM:SS.mmm"
    cri = a.get("datahoracriacao", "")
    atu = a.get("ult_atualizacao", "")
    return (
        f"{emoji_nivel(a.get('nivel'))} <b>{a.get('uf','')}</b> {a.get('municipio','')}\n"
        f"{tipologia(a.get('evento',''))} | <b>{a.get('nivel','')}</b>\n"
        f"Evento: {a.get('evento','')}\n"
        f"IBGE: {a.get('codibge','')} | cod_alerta: {a.get('cod_alerta','')}\n"
        f"Criado: {cri}\n"
        f"Atual.: {atu}"
    )

def main() -> int:
    data = http_get_json(CEMADEN_URL)
    alertas = data.get("alertas", [])
    atualizado = data.get("atualizado", "")

    # só vigentes
    alertas = [a for a in alertas if a.get("status") == 1]

    state = load_state(STATE_PATH)
    seen = state.get("seen", {})  # cod_alerta -> ult_atualizacao

    novos = []
    atualizados = []
    vigentes_agora = {}

    for a in alertas:
        cod = str(a.get("cod_alerta"))
        ult = str(a.get("ult_atualizacao"))
        vigentes_agora[cod] = ult

        if cod not in seen:
            novos.append(a)
        elif seen.get(cod) != ult:
            atualizados.append(a)

    encerrados = []
    for cod in list(seen.keys()):
        if cod not in vigentes_agora:
            encerrados.append(cod)

    # ordena para mensagem ficar boa
    def sort_key(a):
        return (-nivel_rank(a.get("nivel","")), a.get("uf",""), a.get("municipio",""))

    novos.sort(key=sort_key)
    atualizados.sort(key=sort_key)

    # monta mensagem
    if not novos and not atualizados:
        print("Sem novidades.")
    else:
        now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
        parts = [f"📡 <b>CEMADEN</b> | atualização {now_brt}\nConjunto: {atualizado}"]

        if novos:
            parts.append(f"\n<b>Novos alertas vigentes ({len(novos)}):</b>")
            for a in novos[:30]:
                parts.append("\n" + fmt_alert(a))
            if len(novos) > 30:
                parts.append(f"\n... e mais {len(novos)-30} novos (cortei pra não virar bíblia).")

        if atualizados:
            parts.append(f"\n<b>Alertas atualizados ({len(atualizados)}):</b>")
            for a in atualizados[:30]:
                parts.append("\n" + fmt_alert(a))
            if len(atualizados) > 30:
                parts.append(f"\n... e mais {len(atualizados)-30} atualizados.")

        # opcional: avisar encerrados (só ids)
        # se quiser, eu mudo pra mostrar município/UF também, mas aí precisamos guardar mais coisa no state
        if encerrados:
            parts.append(f"\n<b>Encerrados desde a última checagem ({len(encerrados)}):</b> " + ", ".join(encerrados[:50]))
            if len(encerrados) > 50:
                parts.append(f"... +{len(encerrados)-50}")

        tg_send("\n".join(parts))

    # atualiza state
    state["seen"] = vigentes_agora
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(STATE_PATH, state)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
