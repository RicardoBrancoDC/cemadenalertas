#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import uuid
import urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Any, Optional

# =========================
# CONFIG
# =========================

CEMADEN_URL = os.environ.get(
    "CEMADEN_URL",
    "https://painelalertas.cemaden.gov.br/wsAlertas2",
).strip()

STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()
UF_GEOJSON_PATH = os.environ.get("UF_GEOJSON_PATH", "resources/br_uf.geojson").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "30"))
SLEEP_BETWEEN_SENDS_SEC = float(os.environ.get("SLEEP_BETWEEN_SENDS_SEC", "1.2"))

TG_MAX_RETRIES = int(os.environ.get("TG_MAX_RETRIES", "6"))
TG_EXTRA_BACKOFF_SEC = float(os.environ.get("TG_EXTRA_BACKOFF_SEC", "1.0"))

MAX_TG_MESSAGE_LEN = 4096
SEND_MAPS = os.environ.get("SEND_MAPS", "1").strip() == "1"
SEND_ONLY_ON_CHANGE = os.environ.get("SEND_ONLY_ON_CHANGE", "1").strip() == "1"

HISTORY_HOURS = int(os.environ.get("HISTORY_HOURS", "72"))

TZ = ZoneInfo("America/Sao_Paulo")
ALERT_SOURCE_TZ = timezone.utc

LEVEL_COLORS = {
    "Moderado": "#FFD54F",
    "Alto": "#FB8C00",
    "Muito Alto": "#D32F2F",
}

LEVEL_ORDER = {
    "Moderado": 1,
    "Alto": 2,
    "Muito Alto": 3,
}

LEVEL_SIZES = {
    "Moderado": 42,
    "Alto": 72,
    "Muito Alto": 112,
}

# =========================
# HTTP / STATE
# =========================


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)



def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/7.0"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)



def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()

        if not content:
            print(f"Arquivo vazio, recriando: {path}")
            return default

        return json.loads(content)
    except Exception as e:
        print(f"Falha ao ler JSON {path}: {e}")
        print("Vou seguir com o valor padrão.")
        return default



def save_json_file(path: str, data: Any) -> None:
    ensure_parent_dir(path)

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)



def load_state(path: str) -> dict:
    st = load_json_file(
        path,
        {
            "last_conjunto": None,
            "last_run": None,
            "alerts_history": {},
            "last_open_signature": None,
        },
    )
    st.setdefault("last_conjunto", None)
    st.setdefault("last_run", None)
    st.setdefault("alerts_history", {})
    st.setdefault("last_open_signature", None)
    return st



def save_state(path: str, data: dict) -> None:
    save_json_file(path, data)


# =========================
# TELEGRAM
# =========================


def _tg_request_json(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)



def split_message(text: str, max_len: int) -> List[str]:
    if len(text) <= max_len:
        return [text]

    parts: List[str] = []
    cur = ""
    for line in text.split("\n"):
        add = line + "\n"
        if len(cur) + len(add) <= max_len:
            cur += add
        else:
            if cur.strip():
                parts.append(cur.rstrip("\n"))
            cur = add
            if len(cur) > max_len:
                s = cur
                cur = ""
                for i in range(0, len(s), max_len):
                    parts.append(s[i:i + max_len])
    if cur.strip():
        parts.append(cur.rstrip("\n"))
    return parts



def _tg_send_text_with_retry(text: str) -> None:
    last_err = None
    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            _tg_request_json(
                "sendMessage",
                {
                    "chat_id": int(TG_CHAT_ID),
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )
            return
        except Exception as e:
            last_err = e
            backoff = (2 ** (attempt - 1)) + TG_EXTRA_BACKOFF_SEC
            print(f"Falha ao enviar msg Telegram ({attempt}/{TG_MAX_RETRIES}): {e}")
            time.sleep(min(backoff, 30.0))
    raise last_err



def tg_send_text(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Não vou enviar nada.")
        return

    parts = split_message(text, MAX_TG_MESSAGE_LEN)
    for i, part in enumerate(parts, start=1):
        _tg_send_text_with_retry(part)
        if i < len(parts):
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)



def tg_send_photo(photo_path: str, caption: str = "") -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não definidos. Não vou enviar nada.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto"
    boundary = "----cemadenwatch-" + uuid.uuid4().hex
    crlf = "\r\n"

    def part_field(name: str, value: str) -> bytes:
        return (
            f"--{boundary}{crlf}"
            f'Content-Disposition: form-data; name="{name}"{crlf}{crlf}'
            f"{value}{crlf}"
        ).encode("utf-8")

    filename = os.path.basename(photo_path)
    with open(photo_path, "rb") as f:
        file_bytes = f.read()

    file_part_header = (
        f"--{boundary}{crlf}"
        f'Content-Disposition: form-data; name="photo"; filename="{filename}"{crlf}'
        f"Content-Type: image/png{crlf}{crlf}"
    ).encode("utf-8")

    end = f"{crlf}--{boundary}--{crlf}".encode("utf-8")

    body = b"".join(
        [
            part_field("chat_id", str(int(TG_CHAT_ID))),
            part_field("caption", (caption or "")[:900]),
            file_part_header,
            file_bytes,
            end,
        ]
    )

    last_err = None
    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                _ = resp.read()
            return
        except Exception as e:
            last_err = e
            backoff = (2 ** (attempt - 1)) + TG_EXTRA_BACKOFF_SEC
            print(f"Falha ao enviar foto Telegram ({attempt}/{TG_MAX_RETRIES}): {e}")
            time.sleep(min(backoff, 30.0))
    raise last_err


# =========================
# NORMALIZAÇÃO / DATAS / REGRAS
# =========================


def norm(s: Any) -> str:
    return str(s or "").strip()



def parse_alert_dt(s: str) -> Optional[datetime]:
    txt = norm(s)
    if not txt:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(txt, fmt).replace(tzinfo=ALERT_SOURCE_TZ)
        except Exception:
            continue
    return None



def dt_to_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None



def iso_to_dt(s: str) -> Optional[datetime]:
    txt = norm(s)
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        return None



def fmt_dt_local(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M")



def evento_tipo_bruto(evento: str) -> str:
    txt = norm(evento)
    if " - " in txt:
        return txt.split(" - ", 1)[0].strip()
    return txt



def tipo_evento(evento: str) -> Optional[str]:
    base = evento_tipo_bruto(evento).strip().lower()
    if base in {"hidrológico", "hidrologico"}:
        return "hidrologico"
    if base == "movimento de massa":
        return "geologico"
    return None



def color_for_level(nivel: str) -> str:
    return LEVEL_COLORS.get(norm(nivel), "#9E9E9E")



def size_for_level(nivel: str) -> int:
    return LEVEL_SIZES.get(norm(nivel), 35)



def emoji_nivel(nivel: str) -> str:
    n = norm(nivel)
    if n == "Muito Alto":
        return "🟥"
    if n == "Alto":
        return "🟧"
    if n == "Moderado":
        return "🟨"
    return "⬜"


# =========================
# GEOJSON BRASIL / UF
# =========================


def load_uf_geojson(path: str) -> List[dict]:
    if not os.path.exists(path):
        raise RuntimeError(f"UF_GEOJSON_PATH não encontrado: {path}")
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    feats = gj.get("features", [])
    if not feats:
        raise RuntimeError("GeoJSON de UFs vazio.")
    return feats



def geom_to_rings(geometry: dict) -> List[List[Tuple[float, float]]]:
    if not geometry:
        return []

    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    rings: List[List[Tuple[float, float]]] = []

    if gtype == "Polygon":
        if coords and len(coords) > 0:
            outer = coords[0]
            rings.append([(float(p[0]), float(p[1])) for p in outer])
    elif gtype == "MultiPolygon":
        for poly in coords or []:
            if poly and len(poly) > 0:
                outer = poly[0]
                rings.append([(float(p[0]), float(p[1])) for p in outer])

    return rings



def brazil_bbox_from_ufs(uf_features: List[dict]) -> Tuple[float, float, float, float]:
    allx: List[float] = []
    ally: List[float] = []
    for feat in uf_features:
        for ring in geom_to_rings(feat.get("geometry")):
            allx.extend([p[0] for p in ring])
            ally.extend([p[1] for p in ring])

    if not allx or not ally:
        return (-74.0, -34.0, -34.0, 6.0)

    return (min(allx), max(allx), min(ally), max(ally))


# =========================
# HISTÓRICO DOS ALERTAS
# =========================


def merge_current_feed_into_history(history: Dict[str, dict], current_alerts: List[dict], now_utc: datetime) -> Dict[str, dict]:
    for a in current_alerts:
        cod = norm(a.get("cod_alerta"))
        if not cod:
            continue

        created_dt = parse_alert_dt(a.get("datahoracriacao"))
        updated_dt = parse_alert_dt(a.get("ult_atualizacao"))

        prev = history.get(cod, {})
        first_seen_at = prev.get("first_seen_at") or now_utc.isoformat()

        history[cod] = {
            "cod_alerta": cod,
            "datahoracriacao": norm(a.get("datahoracriacao")),
            "ult_atualizacao": norm(a.get("ult_atualizacao")),
            "codibge": norm(a.get("codibge")),
            "evento": norm(a.get("evento")),
            "nivel": norm(a.get("nivel")),
            "status": a.get("status"),
            "uf": norm(a.get("uf")),
            "municipio": norm(a.get("municipio")),
            "latitude": a.get("latitude"),
            "longitude": a.get("longitude"),
            "created_at_iso": dt_to_iso(created_dt),
            "updated_at_iso": dt_to_iso(updated_dt),
            "first_seen_at": first_seen_at,
            "last_seen_at": now_utc.isoformat(),
        }

    keep: Dict[str, dict] = {}
    min_dt = now_utc - timedelta(hours=HISTORY_HOURS)

    for cod, item in history.items():
        created_dt = iso_to_dt(item.get("created_at_iso", ""))
        last_seen_dt = iso_to_dt(item.get("last_seen_at", ""))
        ref_dt = created_dt or last_seen_dt
        if ref_dt and ref_dt >= min_dt:
            keep[cod] = item

    return keep


# =========================
# AGREGAÇÃO PARA MAPAS
# =========================


def build_category_points(alerts: List[dict], category: str) -> List[dict]:
    result: List[dict] = []

    for a in alerts:
        tp = tipo_evento(a.get("evento"))
        if tp != category:
            continue

        nivel = norm(a.get("nivel"))
        if nivel not in LEVEL_ORDER:
            continue

        lat = a.get("latitude")
        lon = a.get("longitude")

        try:
            latf = float(lat)
            lonf = float(lon)
        except Exception:
            continue

        created_dt = parse_alert_dt(a.get("datahoracriacao"))
        created_iso = dt_to_iso(created_dt) or norm(a.get("created_at_iso"))

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



def count_levels_from_points(points: List[dict]) -> Dict[str, int]:
    counts = {"Muito Alto": 0, "Alto": 0, "Moderado": 0}
    for item in points:
        niv = norm(item.get("nivel"))
        if niv in counts:
            counts[niv] += 1
    return counts



def count_open_alerts_by_category_and_level(open_alerts: List[dict]) -> Dict[str, Dict[str, int]]:
    result = {
        "hidrologico": {"Muito Alto": 0, "Alto": 0, "Moderado": 0},
        "geologico": {"Muito Alto": 0, "Alto": 0, "Moderado": 0},
    }

    for a in open_alerts:
        if a.get("status") != 1:
            continue

        categoria = tipo_evento(a.get("evento"))
        nivel = norm(a.get("nivel"))

        if categoria in result and nivel in result[categoria]:
            result[categoria][nivel] += 1

    return result



def build_open_signature(alerts: List[dict]) -> str:
    parts = []
    for a in alerts:
        if a.get("status") != 1:
            continue

        cod = norm(a.get("cod_alerta"))
        nivel = norm(a.get("nivel"))
        evento = norm(a.get("evento"))
        municipio = norm(a.get("municipio"))
        uf = norm(a.get("uf"))
        lat = norm(a.get("latitude"))
        lon = norm(a.get("longitude"))
        parts.append(f"{cod}|{evento}|{nivel}|{municipio}|{uf}|{lat}|{lon}")

    parts.sort()
    return "||".join(parts)


# =========================
# TEXTO / RESUMO
# =========================


def summarize_open_alerts(current_vigentes: List[dict], now_utc: datetime) -> str:
    hid = build_category_points(current_vigentes, "hidrologico")
    geo = build_category_points(current_vigentes, "geologico")

    hid_counts = count_levels_from_points(hid)
    geo_counts = count_levels_from_points(geo)

    total_plotados = len(hid) + len(geo)

    lines = [
        "📊 CEMADEN - alertas abertos",
        f"Atualizado em: {fmt_dt_local(now_utc)}",
        "",
        f"Alertas abertos no feed: {len(current_vigentes)}",
        f"Alertas plotados nos mapas: {total_plotados}",
        "",
        f"🌊 Hidrológico: {len(hid)}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {hid_counts['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {hid_counts['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {hid_counts['Moderado']}",
        "",
        f"⛰️ Geológico: {len(geo)}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {geo_counts['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {geo_counts['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {geo_counts['Moderado']}",
    ]
    return "\n".join(lines)


# =========================
# MAPAS
# =========================


def draw_dual_legend(ax, open_counts: Dict[str, Dict[str, int]]) -> None:
    from matplotlib.patches import Circle

    x0 = 0.025
    y0 = 0.27
    dy = 0.045
    r = 0.0085

    ax.text(
        x0,
        y0,
        "Hidro",
        transform=ax.transAxes,
        fontsize=10,
        fontweight="bold",
        va="top",
        ha="left",
        zorder=11,
    )

    for i, nivel in enumerate(["Muito Alto", "Alto", "Moderado"], start=1):
        y = y0 - i * dy
        circ = Circle(
            (x0 + 0.012, y + 0.003),
            r,
            transform=ax.transAxes,
            facecolor=LEVEL_COLORS[nivel],
            edgecolor="black",
            linewidth=0.5,
            zorder=11,
        )
        ax.add_patch(circ)
        ax.text(
            x0 + 0.03,
            y,
            f"{nivel}: {open_counts['hidrologico'][nivel]}",
            transform=ax.transAxes,
            fontsize=9,
            va="bottom",
            ha="left",
            color="black",
            zorder=11,
        )

    y_geo = y0 - 4.6 * dy
    ax.text(
        x0,
        y_geo,
        "Geo",
        transform=ax.transAxes,
        fontsize=10,
        fontweight="bold",
        va="top",
        ha="left",
        zorder=11,
    )

    for i, nivel in enumerate(["Muito Alto", "Alto", "Moderado"], start=1):
        y = y_geo - i * dy
        circ = Circle(
            (x0 + 0.012, y + 0.003),
            r,
            transform=ax.transAxes,
            facecolor=LEVEL_COLORS[nivel],
            edgecolor="black",
            linewidth=0.5,
            zorder=11,
        )
        ax.add_patch(circ)
        ax.text(
            x0 + 0.03,
            y,
            f"{nivel}: {open_counts['geologico'][nivel]}",
            transform=ax.transAxes,
            fontsize=9,
            va="bottom",
            ha="left",
            color="black",
            zorder=11,
        )

    ax.text(
        x0,
        y0 + 0.05,
        "Resumo dos alertas abertos",
        transform=ax.transAxes,
        fontsize=10,
        fontweight="bold",
        va="bottom",
        ha="left",
        bbox=dict(boxstyle="round,pad=0.45", facecolor="white", edgecolor="#9E9E9E", alpha=0.96),
        zorder=10,
    )



def render_category_map(
    category_name: str,
    points: List[dict],
    uf_features: List[dict],
    out_path: str,
    now_utc: datetime,
    open_counts: Dict[str, Dict[str, int]],
) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(11.8, 11.2))
    ax = fig.add_subplot(111)
    ax.set_facecolor("white")

    for feat in uf_features:
        for ring in geom_to_rings(feat.get("geometry")):
            xs = [p[0] for p in ring]
            ys = [p[1] for p in ring]
            ax.plot(xs, ys, color="#9E9E9E", linewidth=0.5, zorder=1)

    for nivel in ["Moderado", "Alto", "Muito Alto"]:
        subset = [p for p in points if norm(p.get("nivel")) == nivel]
        if not subset:
            continue

        xs = [p["longitude"] for p in subset]
        ys = [p["latitude"] for p in subset]

        ax.scatter(
            xs,
            ys,
            s=size_for_level(nivel),
            c=color_for_level(nivel),
            edgecolors="black",
            linewidths=0.4,
            alpha=0.88,
            zorder=3,
        )

    xmin, xmax, ymin, ymax = brazil_bbox_from_ufs(uf_features)
    padx = (xmax - xmin) * 0.03
    pady = (ymax - ymin) * 0.03
    ax.set_xlim(xmin - padx, xmax + padx)
    ax.set_ylim(ymin - pady, ymax + pady)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])

    pretty_name = "Hidrológicos" if category_name == "hidrologico" else "Geológicos"
    total_alertas = len(points)

    ax.set_title(
        f"CEMADEN - Alertas {pretty_name} abertos\n"
        f"Atualizado em: {fmt_dt_local(now_utc)}",
        fontsize=13,
        pad=16,
    )

    draw_dual_legend(ax, open_counts)

    counts = count_levels_from_points(points)
    summary_lines = [
        f"Categoria do mapa: {pretty_name}",
        f"Alertas plotados: {total_alertas}",
        f"Muito Alto: {counts['Muito Alto']}",
        f"Alto: {counts['Alto']}",
        f"Moderado: {counts['Moderado']}",
    ]

    ax.text(
        0.985,
        0.04,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.45", facecolor="white", edgecolor="#9E9E9E", alpha=0.96),
        zorder=10,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# =========================
# MAIN
# =========================


def main() -> int:
    state = load_state(STATE_PATH)
    now_utc = datetime.now(timezone.utc)

    data = http_get_json(CEMADEN_URL)
    conjunto_atualizado = norm(data.get("atualizado"))
    current_alerts = data.get("alertas", []) or []

    state["alerts_history"] = merge_current_feed_into_history(
        state.get("alerts_history", {}),
        current_alerts,
        now_utc,
    )

    current_vigentes = [a for a in current_alerts if a.get("status") == 1]

    hid_points = build_category_points(current_vigentes, "hidrologico")
    geo_points = build_category_points(current_vigentes, "geologico")
    open_counts = count_open_alerts_by_category_and_level(current_vigentes)

    uf_features = load_uf_geojson(UF_GEOJSON_PATH)

    out_hid = "/tmp/mapa_cemaden_hidrologico_abertos.png"
    out_geo = "/tmp/mapa_cemaden_geologico_abertos.png"

    render_category_map(
        category_name="hidrologico",
        points=hid_points,
        uf_features=uf_features,
        out_path=out_hid,
        now_utc=now_utc,
        open_counts=open_counts,
    )

    render_category_map(
        category_name="geologico",
        points=geo_points,
        uf_features=uf_features,
        out_path=out_geo,
        now_utc=now_utc,
        open_counts=open_counts,
    )

    signature_open = build_open_signature(current_vigentes)

    should_send = True
    if SEND_ONLY_ON_CHANGE:
        should_send = signature_open != norm(state.get("last_open_signature"))

    if should_send:
        tg_send_text(summarize_open_alerts(current_vigentes, now_utc))

        if SEND_MAPS:
            tg_send_photo(
                out_hid,
                caption=(
                    "CEMADEN - Mapa Hidrológico de alertas abertos\n"
                    f"Gerado em: {fmt_dt_local(now_utc)}"
                ),
            )
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            tg_send_photo(
                out_geo,
                caption=(
                    "CEMADEN - Mapa Geológico de alertas abertos\n"
                    f"Gerado em: {fmt_dt_local(now_utc)}"
                ),
            )
    else:
        print("Sem alteração nos alertas abertos. Não vou reenviar Telegram.")

    state["last_conjunto"] = conjunto_atualizado
    state["last_run"] = now_utc.isoformat()
    state["last_open_signature"] = signature_open
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
