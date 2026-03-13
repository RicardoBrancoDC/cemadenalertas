#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CEMADEN Watch - Alertas abertos

Gera dois mapas com base no feed atual do CEMADEN:
1) mapa_cemaden_hidrologico_abertos.png
2) mapa_cemaden_geologico_abertos.png

Regras:
- Usa somente alertas abertos (status == 1)
- Separa por categoria:
    - Hidrológico (evento contendo "hidrol")
    - Geológico (evento contendo "massa")
- Mantém cores por nível:
    - Muito Alto
    - Alto
    - Moderado
- A legenda mostra um resumo geral com:
    - Hidro: quantidade por nível
    - Geo: quantidade por nível
- Pode enviar resumo e mapas para Telegram
- Pode enviar somente quando houver mudança nos alertas abertos
"""

import json
import math
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.patches import FancyBboxPatch

# =========================
# Config via env
# =========================

CEMADEN_URL = os.getenv("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2")
UF_GEOJSON_PATH = os.getenv("UF_GEOJSON_PATH", "resources/br_uf.geojson")
STATE_PATH = os.getenv("STATE_PATH", "state/cemaden_seen.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

REQUEST_TIMEOUT_SEC = int(float(os.getenv("REQUEST_TIMEOUT_SEC", "30")))
SEND_MAPS = os.getenv("SEND_MAPS", "1").strip() == "1"
SEND_ONLY_ON_CHANGE = os.getenv("SEND_ONLY_ON_CHANGE", "1").strip() == "1"
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "1.2"))
TG_MAX_RETRIES = int(float(os.getenv("TG_MAX_RETRIES", "6")))
TG_EXTRA_BACKOFF_SEC = float(os.getenv("TG_EXTRA_BACKOFF_SEC", "1.0"))

OUTPUT_HIDRO = "mapa_cemaden_hidrologico_abertos.png"
OUTPUT_GEO = "mapa_cemaden_geologico_abertos.png"

# =========================
# Visual
# =========================

LEVEL_ORDER = ["Muito Alto", "Alto", "Moderado"]

LEVEL_COLORS = {
    "Muito Alto": "#d73027",
    "Alto": "#fc8d59",
    "Moderado": "#ffd54f",
}

LEVEL_SIZES = {
    "Muito Alto": 130,
    "Alto": 95,
    "Moderado": 70,
}

# =========================
# Utils
# =========================

def norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def norm_lower(value: Any) -> str:
    return norm(value).lower()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def load_json_file(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data: Any) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state(path: str) -> dict:
    state = load_json_file(
        path,
        {
            "last_open_signature": None,
            "last_run": None,
            "last_conjunto": None,
        },
    )
    state.setdefault("last_open_signature", None)
    state.setdefault("last_run", None)
    state.setdefault("last_conjunto", None)
    return state


def save_state(path: str, state: dict) -> None:
    save_json_file(path, state)


def fmt_dt_local(dt_utc: datetime) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    local = dt_utc.astimezone(timezone(timedelta(hours=-3)))
    return local.strftime("%d/%m/%Y %H:%M")


def parse_alert_dt(value: Any) -> Optional[datetime]:
    txt = norm(value)
    if not txt:
        return None

    patterns = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S %Z",
        "%d-%m-%Y %H:%M:%S",
    ]

    for pattern in patterns:
        try:
            dt = datetime.strptime(txt, pattern)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


# =========================
# Regras do feed
# =========================

def status_is_open(value: Any) -> bool:
    try:
        return int(value) == 1
    except Exception:
        return norm_lower(value) in {"1", "true", "aberto", "open"}


def normalize_level(value: Any) -> str:
    txt = norm_lower(value)

    if txt in {"muito alto", "muito_alto", "muitoalto"}:
        return "Muito Alto"
    if txt == "alto":
        return "Alto"
    if txt == "moderado":
        return "Moderado"

    return norm(value)


def evento_tipo_bruto(evento: Any) -> str:
    txt = norm(evento)
    if "-" in txt:
        return txt.split("-", 1)[0].strip()
    return txt


def tipo_evento(evento: Any) -> Optional[str]:
    base = norm_lower(evento)

    if "hidrol" in base:
        return "hidrologico"

    if "massa" in base:
        return "geologico"

    return None


def emoji_nivel(nivel: str) -> str:
    if nivel == "Muito Alto":
        return "🔴"
    if nivel == "Alto":
        return "🟠"
    if nivel == "Moderado":
        return "🟡"
    return "⚪"


# =========================
# GeoJSON e mapa
# =========================

def load_uf_geojson(path: str) -> List[dict]:
    data = load_json_file(path, {})
    features = data.get("features", [])
    return features if isinstance(features, list) else []


def extract_polygons_from_geometry(geometry: dict) -> List[List[List[float]]]:
    if not geometry:
        return []

    gtype = geometry.get("type")
    coords = geometry.get("coordinates")

    if gtype == "Polygon":
        return coords if isinstance(coords, list) else []

    if gtype == "MultiPolygon":
        rings = []
        if isinstance(coords, list):
            for polygon in coords:
                if isinstance(polygon, list):
                    rings.extend(polygon)
        return rings

    return []


def draw_geojson_boundaries(ax, uf_features: List[dict]) -> None:
    for feature in uf_features:
        geometry = feature.get("geometry", {})
        polygons = extract_polygons_from_geometry(geometry)

        for ring in polygons:
            if not ring or len(ring) < 3:
                continue
            try:
                poly = MplPolygon(
                    ring,
                    closed=True,
                    fill=False,
                    edgecolor="#9a9a9a",
                    linewidth=0.5,
                    zorder=1,
                )
                ax.add_patch(poly)
            except Exception:
                continue


def set_brazil_extent(ax) -> None:
    ax.set_xlim(-74, -33)
    ax.set_ylim(-34, 6)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])


# =========================
# Dados de alerta
# =========================

def build_category_points(alerts: List[dict], category: str) -> List[dict]:
    result: List[dict] = []

    for a in alerts:
        tp = tipo_evento(a.get("evento"))
        if tp != category:
            continue

        nivel = normalize_level(a.get("nivel"))
        if nivel not in LEVEL_ORDER:
            continue

        try:
            latf = float(a.get("latitude"))
            lonf = float(a.get("longitude"))
        except Exception:
            continue

        created_dt = parse_alert_dt(a.get("datahoracriacao"))
        created_iso = created_dt.isoformat() if created_dt else norm(a.get("datahoracriacao"))

        result.append(
            {
                "cod_alerta": norm(a.get("cod_alerta")),
                "codibge": norm(a.get("codibge")),
                "municipio": norm(a.get("municipio")),
                "uf": norm(a.get("uf")),
                "evento": norm(a.get("evento")),
                "evento_tipo": evento_tipo_bruto(a.get("evento")),
                "nivel": nivel,
                "latitude": latf,
                "longitude": lonf,
                "created_at_iso": created_iso,
            }
        )

    return result


def count_open_alerts_by_category_and_level(open_alerts: List[dict]) -> Dict[str, Dict[str, int]]:
    result = {
        "hidrologico": {"Muito Alto": 0, "Alto": 0, "Moderado": 0},
        "geologico": {"Muito Alto": 0, "Alto": 0, "Moderado": 0},
    }

    for a in open_alerts:
        if not status_is_open(a.get("status")):
            continue

        categoria = tipo_evento(a.get("evento"))
        nivel = normalize_level(a.get("nivel"))

        if categoria in result and nivel in result[categoria]:
            result[categoria][nivel] += 1

    return result


def build_open_signature(alerts: List[dict]) -> str:
    parts = []

    for a in alerts:
        if not status_is_open(a.get("status")):
            continue

        cod = norm(a.get("cod_alerta"))
        evento = norm(a.get("evento"))
        nivel = normalize_level(a.get("nivel"))
        municipio = norm(a.get("municipio"))
        uf = norm(a.get("uf"))
        lat = norm(a.get("latitude"))
        lon = norm(a.get("longitude"))

        parts.append(f"{cod}|{evento}|{nivel}|{municipio}|{uf}|{lat}|{lon}")

    parts.sort()
    return "||".join(parts)


# =========================
# Legenda
# =========================
def add_legend_box(ax, open_counts: Dict[str, Dict[str, int]]) -> None:
    x = 0.015
    y = 0.015
    box_width = 0.24
    box_height = 0.34
    line_gap = 0.042

    rows = [
        ("Resumo dos alertas abertos", None, None),
        ("Hidro", None, None),
        ("Muito Alto", "hidrologico", "Muito Alto"),
        ("Alto", "hidrologico", "Alto"),
        ("Moderado", "hidrologico", "Moderado"),
        ("Geo", None, None),
        ("Muito Alto", "geologico", "Muito Alto"),
        ("Alto", "geologico", "Alto"),
        ("Moderado", "geologico", "Moderado"),
    ]

    patch = FancyBboxPatch(
        (x, y),
        box_width,
        box_height,
        boxstyle="round,pad=0.012",
        transform=ax.transAxes,
        facecolor="white",
        edgecolor="#bcbcbc",
        linewidth=0.8,
        alpha=0.92,
        zorder=20,
    )
    ax.add_patch(patch)

    tx = x + 0.015
    ty = y + box_height - 0.015

    for idx, (label, cat, lvl) in enumerate(rows):
        if idx == 0:
            ax.text(
                tx,
                ty,
                label,
                transform=ax.transAxes,
                fontsize=9.5,
                fontweight="bold",
                va="top",
                ha="left",
                zorder=21,
            )
            ty -= line_gap * 1.15
            continue

        if cat is None and lvl is None:
            ax.text(
                tx,
                ty,
                label,
                transform=ax.transAxes,
                fontsize=9.2,
                fontweight="bold",
                va="top",
                ha="left",
                zorder=21,
            )
            ty -= line_gap
            continue

        color = LEVEL_COLORS[lvl]
        count = open_counts[cat][lvl]

        ax.text(
            tx,
            ty,
            "●",
            color=color,
            transform=ax.transAxes,
            fontsize=11,
            va="top",
            ha="left",
            zorder=21,
        )

        ax.text(
            tx + 0.025,
            ty,
            f"{label}: {count}",
            color="black",
            transform=ax.transAxes,
            fontsize=8.8,
            va="top",
            ha="left",
            zorder=21,
        )
        ty -= line_gap

# =========================
# Plot
# =========================

def plot_points(ax, points: List[dict]) -> None:
    for level in LEVEL_ORDER:
        xs = [p["longitude"] for p in points if p["nivel"] == level]
        ys = [p["latitude"] for p in points if p["nivel"] == level]

        if not xs:
            continue

        ax.scatter(
            xs,
            ys,
            s=LEVEL_SIZES[level],
            c=LEVEL_COLORS[level],
            edgecolors="black",
            linewidths=0.5,
            alpha=0.92,
            zorder=5,
        )


def render_category_map(
    category_name: str,
    points: List[dict],
    uf_features: List[dict],
    out_path: str,
    now_utc: datetime,
    open_counts: Dict[str, Dict[str, int]],
) -> None:
    fig, ax = plt.subplots(figsize=(12.5, 10))

    draw_geojson_boundaries(ax, uf_features)
    set_brazil_extent(ax)
    plot_points(ax, points)
    add_legend_box(ax, open_counts)

    pretty_name = "Hidrológicos" if category_name == "hidrologico" else "Geológicos"

    ax.set_title(
        f"CEMADEN - Alertas {pretty_name} abertos\nAtualizado em: {fmt_dt_local(now_utc)}",
        fontsize=14,
        pad=16,
    )

    total_points = len(points)
    ax.text(
        0.98,
        0.02,
        f"Cidades plotadas neste mapa: {total_points}",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=9,
        color="#333333",
        bbox=dict(facecolor="white", edgecolor="#d0d0d0", alpha=0.85, boxstyle="round,pad=0.25"),
        zorder=30,
    )

    plt.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# =========================
# Telegram
# =========================

def tg_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def tg_send_text(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram não configurado. Pulando envio de texto.")
        return

    payload = json.dumps(
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")

    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                tg_api_url("sendMessage"),
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                _ = resp.read()
            print("Texto enviado ao Telegram com sucesso.")
            return
        except Exception as e:
            print(f"Falha ao enviar texto ao Telegram. Tentativa {attempt}/{TG_MAX_RETRIES}. Erro: {e}")
            time.sleep(TG_EXTRA_BACKOFF_SEC * attempt)


def tg_send_photo(path: str, caption: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram não configurado. Pulando envio de imagem.")
        return

    boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

    with open(path, "rb") as f:
        file_bytes = f.read()

    data = bytearray()
    fields = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": caption,
    }

    for key, value in fields.items():
        data.extend(f"--{boundary}\r\n".encode())
        data.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        data.extend(f"{value}\r\n".encode())

    filename = os.path.basename(path)
    data.extend(f"--{boundary}\r\n".encode())
    data.extend(
        f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'.encode()
    )
    data.extend(b"Content-Type: image/png\r\n\r\n")
    data.extend(file_bytes)
    data.extend(b"\r\n")
    data.extend(f"--{boundary}--\r\n".encode())

    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                tg_api_url("sendPhoto"),
                data=bytes(data),
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                _ = resp.read()
            print(f"Imagem enviada ao Telegram com sucesso: {filename}")
            return
        except Exception as e:
            print(f"Falha ao enviar imagem ao Telegram. Tentativa {attempt}/{TG_MAX_RETRIES}. Erro: {e}")
            time.sleep(TG_EXTRA_BACKOFF_SEC * attempt)


# =========================
# Resumo
# =========================

def summarize_open_alerts(current_vigentes: List[dict], now_utc: datetime) -> str:
    counts = count_open_alerts_by_category_and_level(current_vigentes)

    hidro_total = sum(counts["hidrologico"].values())
    geo_total = sum(counts["geologico"].values())
    total = hidro_total + geo_total

    lines = [
        "📊 CEMADEN - alertas abertos",
        f"Atualizado em: {fmt_dt_local(now_utc)}",
        "",
        f"Total de alertas abertos: {total}",
        "",
        f"🌊 Hidro: {hidro_total}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {counts['hidrologico']['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {counts['hidrologico']['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {counts['hidrologico']['Moderado']}",
        "",
        f"⛰️ Geo: {geo_total}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {counts['geologico']['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {counts['geologico']['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {counts['geologico']['Moderado']}",
    ]

    return "\n".join(lines)


# =========================
# Main
# =========================

def main() -> int:
    state = load_state(STATE_PATH)
    now_utc = datetime.now(timezone.utc)

    print(f"Lendo feed do CEMADEN em: {CEMADEN_URL}")
    data = http_get_json(CEMADEN_URL)

    conjunto_atualizado = norm(data.get("atualizado"))
    current_alerts = data.get("alertas", []) or []

    print(f"Total de alertas no feed: {len(current_alerts)}")
    print(f"Campo 'atualizado' do feed: {conjunto_atualizado}")

    if current_alerts:
        print("Primeiro alerta bruto do feed:")
        print(json.dumps(current_alerts[0], ensure_ascii=False, indent=2))

    current_vigentes = [a for a in current_alerts if status_is_open(a.get("status"))]
    print(f"Alertas abertos identificados: {len(current_vigentes)}")

    hid_points = build_category_points(current_vigentes, "hidrologico")
    geo_points = build_category_points(current_vigentes, "geologico")
    print(f"Pontos hidro plotáveis: {len(hid_points)}")
    print(f"Pontos geo plotáveis: {len(geo_points)}")

    open_counts = count_open_alerts_by_category_and_level(current_vigentes)
    print("Resumo das contagens por categoria e nível:")
    print(json.dumps(open_counts, ensure_ascii=False, indent=2))

    uf_features = load_uf_geojson(UF_GEOJSON_PATH)
    print(f"Total de features no GeoJSON de UFs: {len(uf_features)}")

    render_category_map(
        category_name="hidrologico",
        points=hid_points,
        uf_features=uf_features,
        out_path=OUTPUT_HIDRO,
        now_utc=now_utc,
        open_counts=open_counts,
    )

    render_category_map(
        category_name="geologico",
        points=geo_points,
        uf_features=uf_features,
        out_path=OUTPUT_GEO,
        now_utc=now_utc,
        open_counts=open_counts,
    )

    print(f"Mapa hidro gerado em: {OUTPUT_HIDRO}")
    print(f"Mapa geo gerado em: {OUTPUT_GEO}")

    signature_open = build_open_signature(current_vigentes)

    should_send = True
    if SEND_ONLY_ON_CHANGE:
        should_send = signature_open != norm(state.get("last_open_signature"))

    print(f"Enviar ao Telegram? {'SIM' if should_send else 'NÃO'}")

    if should_send:
        tg_send_text(summarize_open_alerts(current_vigentes, now_utc))

        if SEND_MAPS:
            tg_send_photo(
                OUTPUT_HIDRO,
                caption=f"CEMADEN - Mapa Hidrológico de alertas abertos\nGerado em: {fmt_dt_local(now_utc)}",
            )
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            tg_send_photo(
                OUTPUT_GEO,
                caption=f"CEMADEN - Mapa Geológico de alertas abertos\nGerado em: {fmt_dt_local(now_utc)}",
            )
    else:
        print("Sem alteração nos alertas abertos. Nada será reenviado.")

    state["last_open_signature"] = signature_open
    state["last_run"] = now_utc.isoformat()
    state["last_conjunto"] = conjunto_atualizado
    save_state(STATE_PATH, state)

    print("Execução finalizada com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
