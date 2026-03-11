#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import uuid
import urllib.request
import urllib.error
import urllib.parse
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

MUNICIPAL_CACHE_PATH = os.environ.get(
    "MUNICIPAL_CACHE_PATH",
    "state/municipios_cache.json",
).strip()

IBGE_MALHA_URL_TMPL = os.environ.get(
    "IBGE_MALHA_URL_TMPL",
    "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{cod}?formato=application%2Fvnd.geo%2Bjson",
).strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "30"))
SLEEP_BETWEEN_SENDS_SEC = float(os.environ.get("SLEEP_BETWEEN_SENDS_SEC", "1.2"))

TG_MAX_RETRIES = int(os.environ.get("TG_MAX_RETRIES", "6"))
TG_EXTRA_BACKOFF_SEC = float(os.environ.get("TG_EXTRA_BACKOFF_SEC", "1.0"))

MAX_TG_MESSAGE_LEN = 4096
SEND_MAPS = os.environ.get("SEND_MAPS", "1").strip() == "1"

HISTORY_HOURS = int(os.environ.get("HISTORY_HOURS", "48"))
MAP_WINDOW_HOURS = int(os.environ.get("MAP_WINDOW_HOURS", "24"))

SEND_ONLY_ON_CHANGE = os.environ.get("SEND_ONLY_ON_CHANGE", "1").strip() == "1"

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

# =========================
# HTTP / STATE
# =========================


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/6.3"})
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
            "last_24h_signature": None,
        },
    )
    st.setdefault("last_conjunto", None)
    st.setdefault("last_run", None)
    st.setdefault("alerts_history", {})
    st.setdefault("last_24h_signature", None)
    return st


def save_state(path: str, data: dict) -> None:
    save_json_file(path, data)


def load_municipal_cache(path: str) -> dict:
    cache = load_json_file(path, {"items": {}})
    cache.setdefault("items", {})
    return cache


def save_municipal_cache(path: str, data: dict) -> None:
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


def nivel_rank(nivel: str) -> int:
    return LEVEL_ORDER.get(norm(nivel), 0)


def evento_tipo_bruto(evento: str) -> str:
    txt = norm(evento)
    if " - " in txt:
        return txt.split(" - ", 1)[0].strip()
    return txt


def tipo_evento(evento: str) -> Optional[str]:
    base = evento_tipo_bruto(evento).lower()
    if "hidrol" in base:
        return "hidrologico"
    if "mov" in base or "massa" in base:
        return "geologico"
    return None


def color_for_level(nivel: str) -> str:
    return LEVEL_COLORS.get(norm(nivel), "#9E9E9E")


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
# MALHA MUNICIPAL IBGE
# =========================


def fetch_municipality_geometry(codibge: str) -> Optional[dict]:
    url = IBGE_MALHA_URL_TMPL.format(cod=codibge)

    if "formato=application/vnd.geo+json" in url:
        url = url.replace(
            "formato=application/vnd.geo+json",
            "formato=" + urllib.parse.quote("application/vnd.geo+json", safe=""),
        )

    last_err = None

    for attempt in range(1, 5):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "cemaden-watch/6.3",
                "Accept": "application/json, application/geo+json, application/vnd.geo+json, */*",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                status = getattr(resp, "status", 200)
                content_type = resp.headers.get("Content-Type", "")
                raw_bytes = resp.read()

            raw_text = raw_bytes.decode("utf-8", errors="replace").strip()

            if status < 200 or status >= 300:
                raise RuntimeError(f"HTTP {status}")

            if not raw_text:
                raise RuntimeError("resposta vazia")

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError:
                preview = raw_text[:220].replace("\n", " ").replace("\r", " ")
                raise RuntimeError(
                    f"resposta não-JSON | content-type={content_type!r} | preview={preview!r}"
                )

            if isinstance(data, dict):
                if data.get("type") == "FeatureCollection":
                    feats = data.get("features", []) or []
                    if feats:
                        return feats[0].get("geometry")

                if data.get("type") == "Feature":
                    return data.get("geometry")

                if data.get("type") in ("Polygon", "MultiPolygon"):
                    return data

            raise RuntimeError(f"JSON inesperado para município {codibge}")

        except Exception as e:
            last_err = e
            wait_s = min(2 ** (attempt - 1), 12)
            print(f"Falha ao baixar malha do município {codibge} (tentativa {attempt}/4): {e}")
            time.sleep(wait_s)

    print(f"Falha final ao baixar malha do município {codibge}: {last_err}")
    return None


def get_municipality_geometry(codibge: str, cache: dict) -> Optional[dict]:
    items = cache.setdefault("items", {})
    key = norm(codibge)
    if not key:
        return None

    if key in items and items[key].get("geometry"):
        return items[key]["geometry"]

    geom = fetch_municipality_geometry(key)
    if geom:
        items[key] = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "geometry": geom,
        }
        return geom

    return None


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


def filter_alerts_last_hours(history: Dict[str, dict], now_utc: datetime, hours: int) -> List[dict]:
    cutoff = now_utc - timedelta(hours=hours)
    out: List[dict] = []
    for item in history.values():
        created_dt = iso_to_dt(item.get("created_at_iso", ""))
        if created_dt and created_dt >= cutoff:
            out.append(item)
    return out


# =========================
# AGREGAÇÃO PARA MAPAS
# =========================


def build_category_municipality_map(alerts_24h: List[dict], category: str) -> Dict[str, dict]:
    result: Dict[str, dict] = {}

    for a in alerts_24h:
        tp = tipo_evento(a.get("evento"))
        if tp != category:
            continue

        codibge = norm(a.get("codibge"))
        nivel = norm(a.get("nivel"))

        if not codibge or nivel not in LEVEL_ORDER:
            continue

        prev = result.get(codibge)
        if prev is None or nivel_rank(nivel) > nivel_rank(prev["nivel"]):
            result[codibge] = {
                "codibge": codibge,
                "municipio": norm(a.get("municipio")),
                "uf": norm(a.get("uf")),
                "nivel": nivel,
                "evento": norm(a.get("evento")),
                "evento_tipo": evento_tipo_bruto(a.get("evento")),
                "cod_alerta": norm(a.get("cod_alerta")),
                "created_at_iso": a.get("created_at_iso"),
            }

    return result


def count_levels(muni_map: Dict[str, dict]) -> Dict[str, int]:
    counts = {"Muito Alto": 0, "Alto": 0, "Moderado": 0}
    for item in muni_map.values():
        niv = norm(item.get("nivel"))
        if niv in counts:
            counts[niv] += 1
    return counts


def build_24h_signature(alerts_24h: List[dict]) -> str:
    parts = []
    for a in alerts_24h:
        cod = norm(a.get("cod_alerta"))
        created = norm(a.get("created_at_iso"))
        nivel = norm(a.get("nivel"))
        evento = norm(a.get("evento"))
        parts.append(f"{cod}|{created}|{nivel}|{evento}")
    parts.sort()
    return "||".join(parts)


# =========================
# TEXTO / RESUMO
# =========================


def summarize_24h(alerts_24h: List[dict], current_vigentes: List[dict], now_utc: datetime) -> str:
    hid = build_category_municipality_map(alerts_24h, "hidrologico")
    geo = build_category_municipality_map(alerts_24h, "geologico")

    hid_counts = count_levels(hid)
    geo_counts = count_levels(geo)

    start_dt = now_utc - timedelta(hours=MAP_WINDOW_HOURS)

    lines = [
        "📊 CEMADEN - janela móvel de 24h",
        f"Período: {fmt_dt_local(start_dt)} até {fmt_dt_local(now_utc)}",
        "",
        f"Alertas vigentes no feed agora: {len(current_vigentes)}",
        f"Alertas capturados nas últimas 24h: {len(alerts_24h)}",
        "",
        f"🌊 Hidrológico - municípios: {len(hid)}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {hid_counts['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {hid_counts['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {hid_counts['Moderado']}",
        "",
        f"⛰️ Geológico - municípios: {len(geo)}",
        f"{emoji_nivel('Muito Alto')} Muito Alto: {geo_counts['Muito Alto']}",
        f"{emoji_nivel('Alto')} Alto: {geo_counts['Alto']}",
        f"{emoji_nivel('Moderado')} Moderado: {geo_counts['Moderado']}",
    ]
    return "\n".join(lines)


# =========================
# MAPAS
# =========================


def render_category_map(
    category_name: str,
    muni_map: Dict[str, dict],
    uf_features: List[dict],
    municipal_cache: dict,
    out_path: str,
    now_utc: datetime,
) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Polygon as MplPolygon
    from matplotlib.patches import Patch

    fig = plt.figure(figsize=(11.8, 11.2))
    ax = fig.add_subplot(111)
    ax.set_facecolor("white")

    missing_geoms: List[str] = []

    for codibge, info in muni_map.items():
        geom = get_municipality_geometry(codibge, municipal_cache)
        if not geom:
            missing_geoms.append(codibge)
            continue

        rings = geom_to_rings(geom)
        if not rings:
            missing_geoms.append(codibge)
            continue

        facecolor = color_for_level(info["nivel"])

        for ring in rings:
            patch = MplPolygon(
                ring,
                closed=True,
                facecolor=facecolor,
                edgecolor="#616161",
                linewidth=0.25,
                zorder=2,
            )
            ax.add_patch(patch)

    for feat in uf_features:
        for ring in geom_to_rings(feat.get("geometry")):
            xs = [p[0] for p in ring]
            ys = [p[1] for p in ring]
            ax.plot(xs, ys, color="#424242", linewidth=0.45, zorder=3)

    xmin, xmax, ymin, ymax = brazil_bbox_from_ufs(uf_features)
    padx = (xmax - xmin) * 0.03
    pady = (ymax - ymin) * 0.03
    ax.set_xlim(xmin - padx, xmax + padx)
    ax.set_ylim(ymin - pady, ymax + pady)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])

    pretty_name = "Hidrológicos" if category_name == "hidrologico" else "Geológicos"
    period_start = now_utc - timedelta(hours=MAP_WINDOW_HOURS)

    counts = count_levels(muni_map)
    total_mun = len(muni_map)

    ax.set_title(
        f"CEMADEN - Alertas {pretty_name} nas últimas {MAP_WINDOW_HOURS} horas\n"
        f"Período: {fmt_dt_local(period_start)} até {fmt_dt_local(now_utc)}",
        fontsize=13,
        pad=16,
    )

    legend_handles = [
        Patch(facecolor=LEVEL_COLORS["Moderado"], edgecolor="#616161", label="Moderado"),
        Patch(facecolor=LEVEL_COLORS["Alto"], edgecolor="#616161", label="Alto"),
        Patch(facecolor=LEVEL_COLORS["Muito Alto"], edgecolor="#616161", label="Muito Alto"),
    ]
    leg = ax.legend(
        handles=legend_handles,
        loc="lower left",
        frameon=True,
        framealpha=0.95,
        title="Severidade",
        fontsize=10,
        title_fontsize=10,
    )
    leg.get_frame().set_edgecolor("#9E9E9E")

    summary_lines = [
        f"Municípios alertados: {total_mun}",
        f"Muito Alto: {counts['Muito Alto']}",
        f"Alto: {counts['Alto']}",
        f"Moderado: {counts['Moderado']}",
    ]
    if missing_geoms:
        summary_lines.append(f"Sem geometria: {len(missing_geoms)}")

    ax.text(
        0.985,
        0.04,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.45", facecolor="white", edgecolor="#9E9E9E", alpha=0.96),
        zorder=4,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# =========================
# MAIN
# =========================


def main() -> int:
    state = load_state(STATE_PATH)
    municipal_cache = load_municipal_cache(MUNICIPAL_CACHE_PATH)
    now_utc = datetime.now(timezone.utc)

    data = http_get_json(CEMADEN_URL)
    conjunto_atualizado = norm(data.get("atualizado"))
    current_alerts = data.get("alertas", []) or []

    state["alerts_history"] = merge_current_feed_into_history(
        state.get("alerts_history", {}),
        current_alerts,
        now_utc,
    )

    alerts_24h = filter_alerts_last_hours(state["alerts_history"], now_utc, MAP_WINDOW_HOURS)
    alerts_24h.sort(key=lambda a: (a.get("created_at_iso") or "", a.get("cod_alerta") or ""))

    hid_map = build_category_municipality_map(alerts_24h, "hidrologico")
    geo_map = build_category_municipality_map(alerts_24h, "geologico")

    uf_features = load_uf_geojson(UF_GEOJSON_PATH)

    out_hid = "/tmp/mapa_cemaden_hidrologico_24h.png"
    out_geo = "/tmp/mapa_cemaden_geologico_24h.png"

    render_category_map(
        category_name="hidrologico",
        muni_map=hid_map,
        uf_features=uf_features,
        municipal_cache=municipal_cache,
        out_path=out_hid,
        now_utc=now_utc,
    )

    render_category_map(
        category_name="geologico",
        muni_map=geo_map,
        uf_features=uf_features,
        municipal_cache=municipal_cache,
        out_path=out_geo,
        now_utc=now_utc,
    )

    save_municipal_cache(MUNICIPAL_CACHE_PATH, municipal_cache)

    current_vigentes = [a for a in current_alerts if a.get("status") == 1]
    signature_24h = build_24h_signature(alerts_24h)

    should_send = True
    if SEND_ONLY_ON_CHANGE:
        should_send = signature_24h != norm(state.get("last_24h_signature"))

    if should_send:
        tg_send_text(summarize_24h(alerts_24h, current_vigentes, now_utc))

        if SEND_MAPS:
            tg_send_photo(
                out_hid,
                caption=(
                    "CEMADEN - Mapa Hidrológico 24h\n"
                    f"Gerado em: {fmt_dt_local(now_utc)}"
                ),
            )
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            tg_send_photo(
                out_geo,
                caption=(
                    "CEMADEN - Mapa Geológico 24h\n"
                    f"Gerado em: {fmt_dt_local(now_utc)}"
                ),
            )
    else:
        print("Janela 24h sem alteração. Não vou reenviar Telegram.")

    state["last_conjunto"] = conjunto_atualizado
    state["last_run"] = now_utc.isoformat()
    state["last_24h_signature"] = signature_24h
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
