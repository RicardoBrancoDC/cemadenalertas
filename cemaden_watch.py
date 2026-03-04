#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Any

# =========================
# CONFIG
# =========================

CEMADEN_URL = os.environ.get("CEMADEN_URL", "https://painelalertas.cemaden.gov.br/wsAlertas2").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/cemaden_seen.json").strip()

# GeoJSON das UFs (use o estadosBrasil2.json do painel, mas versionado no seu repo)
UF_GEOJSON_PATH = os.environ.get("UF_GEOJSON_PATH", "resources/estadosBrasil2.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "30"))
SLEEP_BETWEEN_SENDS_SEC = float(os.environ.get("SLEEP_BETWEEN_SENDS_SEC", "1.2"))

TG_MAX_RETRIES = int(os.environ.get("TG_MAX_RETRIES", "6"))
TG_EXTRA_BACKOFF_SEC = float(os.environ.get("TG_EXTRA_BACKOFF_SEC", "1.0"))

# Segurança pra não mandar texto enorme (Telegram)
MAX_TG_MESSAGE_LEN = 4096
MAX_CITIES_PER_LINE = int(os.environ.get("MAX_CITIES_PER_LINE", "35"))

SEND_MAPS = os.environ.get("SEND_MAPS", "1").strip() == "1"

TZ = ZoneInfo("America/Sao_Paulo")

# =========================
# HTTP / STATE
# =========================


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden-watch/5.1"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"seen_ids": {}, "last_conjunto": None, "last_run": None}
    with open(path, "r", encoding="utf-8") as f:
        st = json.load(f)
    if "seen_ids" not in st:
        st["seen_ids"] = {}
    if "last_conjunto" not in st:
        st["last_conjunto"] = None
    return st


def save_state(path: str, data: dict) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
        add = (line + "\n")
        if len(cur) + len(add) <= max_len:
            cur += add
        else:
            if cur.strip():
                parts.append(cur.rstrip("\n"))
            cur = add
            if len(cur) > max_len:
                # fallback: quebra bruto
                s = cur
                cur = ""
                for i in range(0, len(s), max_len):
                    parts.append(s[i : i + max_len])
    if cur.strip():
        parts.append(cur.rstrip("\n"))
    return parts


def _tg_send_text_with_retry(text: str) -> None:
    last_err = None
    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            _ = _tg_request_json(
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
            print(f"Falha ao enviar msg Telegram (tentativa {attempt}/{TG_MAX_RETRIES}): {e}")
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
    """
    Envia imagem via sendPhoto com multipart/form-data.
    """
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

    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )

    last_err = None
    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                _ = resp.read()
            return
        except Exception as e:
            last_err = e
            backoff = (2 ** (attempt - 1)) + TG_EXTRA_BACKOFF_SEC
            print(f"Falha ao enviar foto Telegram (tentativa {attempt}/{TG_MAX_RETRIES}): {e}")
            time.sleep(min(backoff, 30.0))
    raise last_err


# =========================
# NORMALIZAÇÃO / REGRAS
# =========================


def norm(s: Any) -> str:
    return str(s or "").strip()


def nivel_rank(nivel: str) -> int:
    n = norm(nivel).lower()
    if n == "muito alto":
        return 3
    if n == "alto":
        return 2
    if n == "moderado":
        return 1
    return 0


def emoji_nivel_cemaden(nivel: str) -> str:
    # cores do painel: Moderado amarelo, Alto laranja, Muito Alto vermelho
    n = norm(nivel).lower()
    if n == "muito alto":
        return "🟥"
    if n == "alto":
        return "🟧"
    if n == "moderado":
        return "🟨"
    return "⬜"


def tipologia(evento: str) -> str:
    e = norm(evento).lower()
    if "hidrol" in e or "enx" in e or "inu" in e or "ris" in e:
        return "Hidrológico"
    if "mov" in e:
        return "Movimento de Massa"
    return "Outro"


def fmt_dt_short(dt_str: str) -> str:
    """
    Entrada típica: '2026-03-03 03:00:40.051'
    Saída: '03/03 00:00' no BRT (a origem geralmente está em UTC no backend)
    """
    s = norm(dt_str)
    if not s:
        return "-"
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            d = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return d.astimezone(TZ).strftime("%d/%m %H:%M")
        except Exception:
            pass
    return s


# =========================
# GEOJSON UFs (estadosBrasil2.json)
# =========================


def load_ufs_geojson(path: str) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Retorna:
      uf_geom: dict UF -> geometry
      uf_name: dict UF -> nome_uf
    Espera props: uf_05 e nome_uf (como no estadosBrasil2.json do painel).
    """
    if not os.path.exists(path):
        raise RuntimeError(f"UF_GEOJSON_PATH não encontrado: {path}")

    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    feats = gj.get("features", [])
    if not feats:
        raise RuntimeError("GeoJSON de UFs vazio ou sem 'features'.")

    uf_geom: Dict[str, Any] = {}
    uf_name: Dict[str, str] = {}
    for feat in feats:
        props = feat.get("properties", {}) or {}
        uf = norm(props.get("uf_05"))
        nome = norm(props.get("nome_uf"))
        geom = feat.get("geometry")
        if uf and geom:
            uf_geom[uf] = geom
            uf_name[uf] = nome or uf

    if not uf_geom:
        raise RuntimeError("Não consegui extrair geometrias por UF. Verifica se existe 'uf_05' nas properties.")
    return uf_geom, uf_name


def _iter_polygons(geometry: dict) -> List[List[Tuple[float, float]]]:
    """
    Retorna lista de anéis (x,y) para plot.
    GeoJSON usa [lon, lat].
    """
    if not geometry:
        return []
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    polys: List[List[Tuple[float, float]]] = []

    if gtype == "Polygon":
        # coords: [ [ [lon,lat], ... ] , holes... ]
        if coords and len(coords) > 0:
            ring = coords[0]
            polys.append([(p[0], p[1]) for p in ring])
    elif gtype == "MultiPolygon":
        # coords: [ polygon1, polygon2, ... ] onde polygon = [ring1, ring2...]
        for poly in coords or []:
            if poly and len(poly) > 0:
                ring = poly[0]
                polys.append([(p[0], p[1]) for p in ring])
    return polys


def make_state_map_png(
    uf: str,
    uf_geom: Dict[str, Any],
    points: List[Tuple[float, float, str, str]],
    out_path: str,
    title: str,
) -> None:
    """
    points: lista de (lon, lat, label, nivel)
    nivel esperado: 'Moderado' | 'Alto' | 'Muito Alto'
    """
    import matplotlib.pyplot as plt

    geom = uf_geom.get(uf)
    if not geom:
        raise ValueError(f"Sem geometria para UF={uf}")

    rings = _iter_polygons(geom)
    if not rings:
        raise ValueError(f"Geometria vazia para UF={uf}")

    fig = plt.figure(figsize=(6.5, 6.5))
    ax = fig.add_subplot(111)

    # contorno(s) do estado
    for ring in rings:
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        ax.plot(xs, ys)

    # cores do painel
    color_by = {
        "Moderado": "yellow",
        "Alto": "orange",
        "Muito Alto": "red",
    }

    buckets: Dict[str, List[Tuple[float, float]]] = {"Muito Alto": [], "Alto": [], "Moderado": []}
    for lon, lat, _label, nivel in points:
        niv = norm(nivel)
        if niv in buckets:
            buckets[niv].append((lon, lat))

    # plota na ordem leve -> grave, mas o grave fica por cima porque vai por último
    for niv in ["Moderado", "Alto", "Muito Alto"]:
        pts = buckets.get(niv, [])
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.scatter(xs, ys, s=42, c=color_by[niv], label=niv, alpha=0.95)

    if any(len(v) > 0 for v in buckets.values()):
        ax.legend(loc="lower left", frameon=True)

    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal", adjustable="box")

    # bbox do polígono + folga
    allx: List[float] = []
    ally: List[float] = []
    for ring in rings:
        allx.extend([p[0] for p in ring])
        ally.extend([p[1] for p in ring])

    if allx and ally:
        xmin, xmax = min(allx), max(allx)
        ymin, ymax = min(ally), max(ally)
        padx = (xmax - xmin) * 0.05 or 0.2
        pady = (ymax - ymin) * 0.05 or 0.2
        ax.set_xlim(xmin - padx, xmax + padx)
        ax.set_ylim(ymin - pady, ymax + pady)

    fig.tight_layout()
    fig.savefig(out_path, dpi=170)
    plt.close(fig)


# =========================
# MENSAGENS
# =========================


def summarize_panel(all_active: List[dict], conjunto_atualizado: str) -> str:
    total = len(all_active)

    by_level = {"Moderado": 0, "Alto": 0, "Muito Alto": 0}
    by_type = {"Hidrológico": 0, "Movimento de Massa": 0, "Outro": 0}

    for a in all_active:
        niv = norm(a.get("nivel"))
        tp = tipologia(a.get("evento"))
        if niv in by_level:
            by_level[niv] += 1
        else:
            by_level[niv] = by_level.get(niv, 0) + 1

        if tp in by_type:
            by_type[tp] += 1
        else:
            by_type["Outro"] += 1

    lines = [
        "📊 Resumo CEMADEN (vigentes)",
        f"Total: {total}",
        f"{emoji_nivel_cemaden('Muito Alto')} Muito Alto: {by_level.get('Muito Alto', 0)}",
        f"{emoji_nivel_cemaden('Alto')} Alto: {by_level.get('Alto', 0)}",
        f"{emoji_nivel_cemaden('Moderado')} Moderado: {by_level.get('Moderado', 0)}",
        "",
        f"🌊 Hidrológico: {by_type.get('Hidrológico', 0)}",
        f"⛰️ Movimento de Massa: {by_type.get('Movimento de Massa', 0)}",
        "",
        f"Conjunto: {conjunto_atualizado}",
    ]
    return "\n".join(lines)


def build_uf_message(
    uf: str,
    uf_nome: str,
    new_alerts_uf: List[dict],
    conjunto_atualizado: str,
) -> str:
    c_hidro = 0
    c_massa = 0
    for a in new_alerts_uf:
        tp = tipologia(a.get("evento"))
        if tp == "Hidrológico":
            c_hidro += 1
        elif tp == "Movimento de Massa":
            c_massa += 1

    bucket: Dict[str, Dict[str, List[str]]] = {
        "Hidrológico": {"Muito Alto": [], "Alto": [], "Moderado": []},
        "Movimento de Massa": {"Muito Alto": [], "Alto": [], "Moderado": []},
        "Outro": {"Muito Alto": [], "Alto": [], "Moderado": []},
    }

    for a in new_alerts_uf:
        tp = tipologia(a.get("evento"))
        niv = norm(a.get("nivel"))
        if niv not in ("Muito Alto", "Alto", "Moderado"):
            continue

        mun = norm(a.get("municipio"))
        cod = norm(a.get("cod_alerta"))
        dt = fmt_dt_short(a.get("datahoracriacao"))
        item = f"{mun} (Cód {cod} | {dt})"
        bucket.setdefault(tp, {}).setdefault(niv, []).append(item)

    # ordena listas por cidade
    for tp in bucket:
        for niv in bucket[tp]:
            bucket[tp][niv] = sorted(bucket[tp][niv])

    header = [
        f"📣 Alertas {uf_nome} ({uf})",
        f"Novos: {len(new_alerts_uf)} | 🌊 Hidrológico: {c_hidro} | ⛰️ Massa: {c_massa}",
        f"Conjunto: {conjunto_atualizado}",
        "",
    ]

    def fmt_block(tp: str) -> List[str]:
        icon = "🌊" if tp == "Hidrológico" else "⛰️" if tp == "Movimento de Massa" else "ℹ️"
        out = [f"{icon} {tp}:"]
        had_any = False

        for niv in ("Muito Alto", "Alto", "Moderado"):
            items = bucket.get(tp, {}).get(niv, [])
            if not items:
                continue
            had_any = True

            shown = items[:MAX_CITIES_PER_LINE]
            tail = "" if len(items) <= len(shown) else f" (+{len(items)-len(shown)} outros)"
            out.append(f"{emoji_nivel_cemaden(niv)} {niv.upper()}: " + "; ".join(shown) + tail)

        if not had_any:
            out.append("Sem novos itens nesse tipo.")
        out.append("")
        return out

    body: List[str] = []
    if c_hidro:
        body.extend(fmt_block("Hidrológico"))
    if c_massa:
        body.extend(fmt_block("Movimento de Massa"))

    other_count = sum(len(bucket.get("Outro", {}).get(niv, [])) for niv in ("Moderado", "Alto", "Muito Alto"))
    if other_count:
        body.extend(fmt_block("Outro"))

    return "\n".join(header + body).strip()


# =========================
# MAIN
# =========================


def main() -> int:
    state = load_state(STATE_PATH)
    seen_ids: Dict[str, str] = state.get("seen_ids", {})
    last_conjunto = state.get("last_conjunto")

    data = http_get_json(CEMADEN_URL)
    conjunto_atualizado = norm(data.get("atualizado"))

    alertas = data.get("alertas", [])
    vigentes = [a for a in alertas if a.get("status") == 1]

    # regra: só agir quando o conjunto mudar
    if conjunto_atualizado and last_conjunto == conjunto_atualizado:
        print("Conjunto sem alteração. Não vou enviar nada.")
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(STATE_PATH, state)
        return 0

    # novos = ainda não vistos (cod_alerta)
    novos: List[dict] = []
    for a in vigentes:
        cod = norm(a.get("cod_alerta"))
        if cod and cod not in seen_ids:
            novos.append(a)

    # agrupa novos por UF
    novos_por_uf: Dict[str, List[dict]] = {}
    for a in novos:
        uf = norm(a.get("uf"))
        if uf:
            novos_por_uf.setdefault(uf, []).append(a)

    # tenta carregar o geojson das UFs (para mapas e nomes)
    uf_geom: Dict[str, Any] = {}
    uf_nome: Dict[str, str] = {}
    send_maps_local = False
    if SEND_MAPS:
        try:
            uf_geom, uf_nome = load_ufs_geojson(UF_GEOJSON_PATH)
            send_maps_local = True
        except Exception as e:
            print(f"Atenção: não consegui carregar UF_GEOJSON_PATH='{UF_GEOJSON_PATH}': {e}")
            print("Vou seguir sem mapas.")
            send_maps_local = False

    # se não tiver novos, ainda assim manda um aviso leve (porque o conjunto mudou)
    if not novos_por_uf:
        tg_send_text(
            "📣 CEMADEN\n"
            "Conjunto atualizado, mas sem alertas novos desde a última rodada.\n"
            f"Conjunto: {conjunto_atualizado}"
        )
    else:
        # ordena UFs pelo nível máximo (mais grave primeiro)
        def uf_key(item: Tuple[str, List[dict]]) -> Tuple[int, str]:
            uf, arr = item
            maxrank = 0
            for a in arr:
                maxrank = max(maxrank, nivel_rank(a.get("nivel")))
            return (-maxrank, uf)

        for uf, arr in sorted(novos_por_uf.items(), key=uf_key):
            # ordena alertas dentro da UF: grave -> leve, depois tipo e cidade
            arr.sort(
                key=lambda a: (
                    -nivel_rank(a.get("nivel")),
                    tipologia(a.get("evento")),
                    norm(a.get("municipio")),
                )
            )

            nome = uf_nome.get(uf, uf)
            msg = build_uf_message(uf, nome, arr, conjunto_atualizado)
            tg_send_text(msg)
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

            # mapa da UF com pontos por nível
            if send_maps_local and uf in uf_geom:
                pts: List[Tuple[float, float, str, str]] = []
                for a in arr:
                    lat = a.get("latitude")
                    lon = a.get("longitude")
                    if lat is None or lon is None:
                        continue
                    try:
                        latf = float(lat)
                        lonf = float(lon)
                    except Exception:
                        continue
                    pts.append((lonf, latf, norm(a.get("municipio")), norm(a.get("nivel"))))

                if pts:
                    out_png = f"/tmp/map_{uf}.png"
                    title = f"{nome} ({uf}) | novos: {len(arr)}"
                    try:
                        make_state_map_png(uf, uf_geom, pts, out_png, title)
                        tg_send_photo(out_png, caption=f"{nome} ({uf})\nConjunto: {conjunto_atualizado}")
                        time.sleep(SLEEP_BETWEEN_SENDS_SEC)
                    except Exception as e:
                        print(f"Falha ao gerar/enviar mapa da UF {uf}: {e}")

    # resumo geral do painel sempre que o conjunto mudar
    tg_send_text(summarize_panel(vigentes, conjunto_atualizado))

    # atualiza state: marca todos os vigentes como vistos
    for a in vigentes:
        cod = norm(a.get("cod_alerta"))
        ult = norm(a.get("ult_atualizacao"))
        if cod:
            seen_ids[cod] = ult

    state["seen_ids"] = seen_ids
    state["last_conjunto"] = conjunto_atualizado
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(STATE_PATH, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
