#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import geopandas as gpd
import matplotlib.pyplot as plt

CEMADEN_URL = os.environ.get("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()
UF_GEOJSON_PATH = os.environ.get("UF_GEOJSON_PATH", "resources/br_uf.geojson").strip()

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
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/5.0"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"seen": {}, "last_run": None, "last_conjunto": ""}
    with open(path, "r", encoding="utf-8") as f:
        st = json.load(f)
    if "seen" not in st:
        st["seen"] = {}
    if "last_conjunto" not in st or st["last_conjunto"] is None:
        st["last_conjunto"] = ""
    return st


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


def tg_send_photo(path: str, caption: str = "") -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Saindo sem enviar.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto"
    boundary = "----cemadenwatchboundary"

    with open(path, "rb") as f:
        img = f.read()

    parts = []

    def add_field(name: str, value: str):
        parts.append(f"--{boundary}\r\n".encode("utf-8"))
        parts.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        parts.append(value.encode("utf-8"))
        parts.append(b"\r\n")

    def add_file(name: str, filename: str, content: bytes):
        parts.append(f"--{boundary}\r\n".encode("utf-8"))
        parts.append(
            (
                f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                f"Content-Type: image/png\r\n\r\n"
            ).encode("utf-8")
        )
        parts.append(content)
        parts.append(b"\r\n")

    add_field("chat_id", str(TG_CHAT_ID))
    if caption:
        add_field("caption", caption)
        add_field("parse_mode", "Markdown")

    add_file("photo", os.path.basename(path), img)
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))

    body = b"".join(parts)
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC).read()


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


def normalize_conjunto(s: str) -> str:
    return " ".join((s or "").strip().split())


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
    # CEMADEN: Muito Alto = vermelho, Alto = laranja, Moderado = amarelo
    if nivel_key_str == "MUITO ALTO":
        return "🟥 *MUITO ALTO*"
    if nivel_key_str == "ALTO":
        return "🟧 *ALTO*"
    if nivel_key_str == "MODERADO":
        return "🟨 *MODERADO*"
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


def group_new_alerts_by_uf(alertas_novos: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for a in alertas_novos:
        uf = str(a.get("uf", "")).strip() or "??"
        out.setdefault(uf, []).append(a)
    return out


def build_messages_by_uf(alertas_novos: list[dict]) -> dict[str, list[str]]:
    grouped_uf: dict[str, dict[str, dict[str, list[dict]]]] = {}

    for a in alertas_novos:
        uf = str(a.get("uf", "")).strip() or "??"
        tip = tipologia(str(a.get("evento", "")))
        niv = nivel_key(str(a.get("nivel", "")))
        grouped_uf.setdefault(uf, {}).setdefault(tip, {}).setdefault(niv, []).append(a)

    tip_order = ["Hidrológico", "Movimento de Massa", "Outro"]
    niv_order = ["MUITO ALTO", "ALTO", "MODERADO", "OUTRO"]

    result: dict[str, list[str]] = {}

    for uf in sorted(grouped_uf.keys()):
        uf_block = grouped_uf[uf]

        cnt_lvl = {"MUITO ALTO": 0, "ALTO": 0, "MODERADO": 0, "OUTRO": 0}
        cnt_tip = {"Hidrológico": 0, "Movimento de Massa": 0, "Outro": 0}

        for tip, tip_block in uf_block.items():
            for niv, items in tip_block.items():
                cnt_lvl[niv] = cnt_lvl.get(niv, 0) + len(items)
                cnt_tip[tip] = cnt_tip.get(tip, 0) + len(items)

        lines = [
            f"📣 *Alertas {esc_md(uf)}* (novos)",
            f"🟥 {cnt_lvl['MUITO ALTO']}  🟧 {cnt_lvl['ALTO']}  🟨 {cnt_lvl['MODERADO']}",
            f"💧 {cnt_tip.get('Hidrológico', 0)}  ⛰️ {cnt_tip.get('Movimento de Massa', 0)}",
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
        parts = list(chunks_by_lines(text, TG_TEXT_LIMIT))
        result[uf] = parts

    return result


def _load_uf_gdf(path: str) -> tuple[gpd.GeoDataFrame, str]:
    gdf = gpd.read_file(path)

    col = None
    for c in ["sigla", "uf", "UF", "SIGLA", "Uf", "Sigla"]:
        if c in gdf.columns:
            col = c
            break
    if col is None:
        raise RuntimeError("GeoJSON não tem coluna de UF. Esperava algo como: sigla ou uf.")

    # padroniza pra facilitar filtro
    gdf[col] = gdf[col].astype(str).str.upper().str.strip()

    # garante CRS comum
    try:
        if gdf.crs is None:
            # se não tiver CRS, assume WGS84
            gdf = gdf.set_crs("EPSG:4326")
        else:
            gdf = gdf.to_crs("EPSG:4326")
    except Exception:
        pass

    return gdf, col


def gerar_mapa_uf(alertas_uf_novos: list[dict], uf: str, uf_gdf: gpd.GeoDataFrame, col_uf: str) -> str:
    uf = (uf or "").upper().strip()
    uf_row = uf_gdf[uf_gdf[col_uf] == uf]
    if uf_row.empty:
        print(f"UF {uf} não encontrada no GeoJSON, pulando mapa.")
        return ""

    lats = []
    lons = []
    for a in alertas_uf_novos:
        lat = a.get("latitude")
        lon = a.get("longitude")
        if lat is None or lon is None:
            continue
        try:
            lats.append(float(lat))
            lons.append(float(lon))
        except Exception:
            pass

    if not lats:
        return ""

    fig = plt.figure(figsize=(6, 6))
    ax = plt.gca()

    uf_row.boundary.plot(ax=ax, linewidth=1)
    ax.scatter(lons, lats, s=35)

    minx, miny, maxx, maxy = uf_row.total_bounds
    padx = (maxx - minx) * 0.15
    pady = (maxy - miny) * 0.15
    if padx == 0:
        padx = 0.5
    if pady == 0:
        pady = 0.5

    ax.set_xlim(minx - padx, maxx + padx)
    ax.set_ylim(miny - pady, maxy + pady)

    ax.set_title(f"CEMADEN {uf} | alertas novos (pontos)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.tight_layout()
    out = f"map_{uf}.png"
    plt.savefig(out, dpi=160)
    plt.close(fig)
    return out


def build_global_summary(alertas_vigentes: list[dict], conjunto_atualizado: str) -> str:
    cnt_lvl = {"MUITO ALTO": 0, "ALTO": 0, "MODERADO": 0, "OUTRO": 0}
    cnt_tip = {"Hidrológico": 0, "Movimento de Massa": 0, "Outro": 0}

    for a in alertas_vigentes:
        niv = nivel_key(str(a.get("nivel", "")))
        tip = tipologia(str(a.get("evento", "")))
        cnt_lvl[niv] = cnt_lvl.get(niv, 0) + 1
        cnt_tip[tip] = cnt_tip.get(tip, 0) + 1

    total = sum(cnt_lvl.values())
    now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")

    return "\n".join(
        [
            "📊 *Resumo Geral CEMADEN*",
            f"Conjunto: {esc_md(conjunto_atualizado) if conjunto_atualizado else '-'}",
            f"Atualização: {esc_md(now_brt)} (BRT)",
            "",
            f"Total de alertas vigentes: {total}",
            "",
            "*Níveis Abertos*",
            f"🟥 Muito Alto: {cnt_lvl['MUITO ALTO']}",
            f"🟧 Alto: {cnt_lvl['ALTO']}",
            f"🟨 Moderado: {cnt_lvl['MODERADO']}",
            "",
            "*Tipos de Alertas*",
            f"⛰️ Mov. Massa: {cnt_tip.get('Movimento de Massa', 0)}",
            f"💧 Risco Hidrológico: {cnt_tip.get('Hidrológico', 0)}",
        ]
    )


def main() -> int:
    state = load_state(STATE_PATH)
    seen = state.get("seen", {})
    last_conjunto = normalize_conjunto(state.get("last_conjunto", ""))

    data = http_get_json(CEMADEN_URL)
    conjunto_atualizado = normalize_conjunto(str(data.get("atualizado", "")))

    alertas = data.get("alertas", [])
    alertas = [a for a in alertas if a.get("status") == 1]

    novos = []
    for a in alertas:
        cod = str(a.get("cod_alerta"))
        if cod and cod not in seen:
            novos.append(a)

    total_novos = len(novos)
    enviar = novos[:MAX_NEW_ALERTS_PER_RUN]

    # mensagens + mapas só para UFs com novos alertas
    if enviar:
        uf_to_alerts = group_new_alerts_by_uf(enviar)
        uf_to_msgs = build_messages_by_uf(enviar)

        # carrega o geojson uma vez
        uf_gdf, col_uf = _load_uf_gdf(UF_GEOJSON_PATH)

        for uf in sorted(uf_to_msgs.keys()):
            # 1) manda texto da UF
            for i, msg in enumerate(uf_to_msgs[uf], start=1):
                tg_send(msg)
                if i < len(uf_to_msgs[uf]):
                    time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            # 2) manda mapa da UF, só com os pontos dos novos
            map_path = gerar_mapa_uf(uf_to_alerts.get(uf, []), uf, uf_gdf, col_uf)
            if map_path:
                tg_send_photo(map_path, caption=f"🗺️ *Mapa {esc_md(uf)}* | pontos dos alertas novos")
                time.sleep(SLEEP_BETWEEN_SENDS_SEC)

        if total_novos > len(enviar) and SEND_SUMMARY_WHEN_CAPPED:
            now_brt = datetime.now(timezone.utc).astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
            tg_send(
                "⚠️ *CEMADEN*\n"
                f"Foram detectados {total_novos} alertas novos, mas processei só {len(enviar)} nesta rodada.\n"
                f"Atualização: {esc_md(now_brt)} (BRT)\n"
                "Os demais entram nas próximas execuções."
            )
    else:
        print("Sem novos alertas.")

    # resumo: só quando o conjunto mudar, e não dispara na primeira execução
    if not last_conjunto:
        print("Primeira execução: gravando conjunto e não enviando resumo.")
    else:
        conjunto_mudou = (conjunto_atualizado != "" and conjunto_atualizado != last_conjunto)
        if conjunto_mudou:
            tg_send(build_global_summary(alertas, conjunto_atualizado))
        else:
            print("Resumo suprimido: conjunto não mudou.")

    # marca vigentes como vistos
    for a in alertas:
        cod = str(a.get("cod_alerta"))
        ult = str(a.get("ult_atualizacao"))
        if cod:
            seen[cod] = ult

    state["seen"] = seen
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    state["last_conjunto"] = conjunto_atualizado
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
