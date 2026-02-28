import os
import io
import re
import csv
import json
import base64
import random
import asyncio
import logging
import warnings
import urllib.parse
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")              # non-interactive PNG backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.font_manager as _fm
from matplotlib.gridspec import GridSpec

import requests
from bs4 import BeautifulSoup

import cv2
import numpy as np
from pyzbar import pyzbar as _pyzbar

from reportlab.lib import colors as _rl_colors
from reportlab.lib.pagesizes import A4 as _RL_A4
from reportlab.lib.units import cm as _RL_CM
from reportlab.lib.styles import getSampleStyleSheet as _rl_styles
from reportlab.lib.styles import ParagraphStyle as _rl_ParagraphStyle
from reportlab.lib.enums import TA_LEFT as _TA_LEFT, TA_CENTER as _TA_CENTER, TA_RIGHT as _TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate as _rl_Doc,
    Paragraph as _rl_P, Spacer as _rl_Spacer,
    Table as _rl_Table, TableStyle as _rl_TS,
    HRFlowable as _rl_HR,
    Image as _rl_Img,
)

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
load_dotenv()

BOT_TOKEN        = os.getenv("BOT_TOKEN")
ADMIN_AUTH_TOKEN = os.getenv("ADMIN-AUTH-TOKEN", "").strip()

ROLES: dict[str, str] = {}
if ADMIN_AUTH_TOKEN:
    ROLES[ADMIN_AUTH_TOKEN] = "admin"

ROLE_PERMISSIONS: dict[str, list[str]] = {
    "admin":     ["add_user_role", "remove_user_role", "view_logs",
                  "request_resume", "see_users_sending_messages",
                  "request_myid", "send_nf", "help_message"],
    "moderator": ["request_resume", "send_nf", "request_myid", "help_message"],
    "user":      ["request_myid", "help_message"],
}

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Subdirectories for outputs
(OUTPUT_DIR / "pdf").mkdir(exist_ok=True)
(OUTPUT_DIR / "json").mkdir(exist_ok=True)
(OUTPUT_DIR / "png").mkdir(exist_ok=True)
(OUTPUT_DIR / "txt").mkdir(exist_ok=True)
(OUTPUT_DIR / "db").mkdir(exist_ok=True)

OUTPUT_QR  = Path("NF_QR_CODE")
OUTPUT_QR.mkdir(exist_ok=True)

LOGO_PATH  = (Path("profile_banenr")
              / "Gemini_Generated_Image_gtsx73gtsx73gtsx-removebg-preview.png")

ROLES_FILE    = Path("roles.json")
ACTIVITY_FILE = Path("activity.json")
CSV_SCANS     = OUTPUT_DIR / "db" / "scans.csv"
CSV_PRODUTOS  = OUTPUT_DIR / "db" / "produtos.csv"
CSV_LOJAS     = OUTPUT_DIR / "db" / "lojas.csv"


# ──────────────────────────────────────────────
# CSV index
# ──────────────────────────────────────────────
_LOJA_FIELDS = ["loja_id", "cnpj", "nome", "endereco"]

_SCAN_FIELDS = [
    "scan_id", "scan_at", "user_id", "qr_url", "qr_json_path", "chave_acesso",
    "loja_id",
    "ambiente", "uf", "modelo", "serie", "numero_nf", "cnpj_emit",
    "emitente_nome", "emitente_endereco", "data_emissao",
    "numero_protocolo", "data_protocolo",
    "valor_total", "valor_desconto", "valor_pagar", "valor_icms",
    "tributos_total", "qtd_itens", "forma_pagamento",
    "consumidor", "versao_xml", "versao_xslt",
    "data_hora_page",
]
_PROD_FIELDS = [
    "scan_id", "loja_id", "data_hora_page",
    "emitente_nome", "emitente_endereco",
    "valor_total_scan", "valor_desconto_scan",
    "descricao", "codigo", "quantidade",
    "unidade", "valor_unitario", "valor_total",
]


def _get_or_create_loja(cnpj: str | None, nome: str | None, endereco: str | None) -> str:
    """Return loja_id (e.g. 'L001') for the given CNPJ/name, creating a new entry if needed."""
    key = (cnpj or "").strip() or (nome or "").strip() or "DESCONHECIDO"
    # Load existing lojas
    rows: list[dict] = []
    if CSV_LOJAS.exists() and CSV_LOJAS.stat().st_size > 0:
        with CSV_LOJAS.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    # Look for existing entry by CNPJ (primary) or name fallback
    for r in rows:
        if cnpj and r.get("cnpj", "").strip() == cnpj.strip():
            return r["loja_id"]
        if not cnpj and nome and r.get("nome", "").strip() == nome.strip():
            return r["loja_id"]
    # Assign next sequential ID
    next_num = len(rows) + 1
    loja_id  = f"L{next_num:03d}"
    new_row  = {"loja_id": loja_id, "cnpj": cnpj or "", "nome": nome or "", "endereco": endereco or ""}
    new_file = not CSV_LOJAS.exists() or CSV_LOJAS.stat().st_size == 0
    with CSV_LOJAS.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_LOJA_FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(new_row)
    return loja_id


def _clean_address(raw: str | None) -> str | None:
    """Normalize scraped address: filter empty comma-separated fragments."""
    if not raw:
        return None
    parts = [p.strip() for p in re.split(r"[,\n]+", raw)]
    parts = [p for p in parts if p and p not in {",", ".", "-"}]
    return ", ".join(parts) if parts else None


def _next_scan_id() -> int:
    """Return next sequential scan ID by counting existing rows."""
    if not CSV_SCANS.exists():
        return 1
    with CSV_SCANS.open(encoding="utf-8", newline="") as f:
        # Count data rows (excludes header)
        n = sum(1 for _ in csv.reader(f)) - 1
    return max(n + 1, 1)


def _csv_append(path: Path, fields: list[str], row: dict) -> None:
    """Append one row to a CSV, writing header if file is new."""
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(row)


def _save_to_csv(scan_record: dict, produtos: list[dict]) -> str:
    """Write scan + products to CSV files. Returns the scan_id string."""
    scan_id = str(_next_scan_id())
    scan_record["scan_id"] = scan_id
    _csv_append(CSV_SCANS, _SCAN_FIELDS, scan_record)
    for p in produtos:
        p2 = dict(p)
        p2["scan_id"]             = scan_id
        p2["loja_id"]             = scan_record.get("loja_id")
        p2["data_hora_page"]      = scan_record.get("data_hora_page")
        p2["emitente_nome"]       = scan_record.get("emitente_nome")
        p2["emitente_endereco"]   = scan_record.get("emitente_endereco")
        p2["valor_total_scan"]    = scan_record.get("valor_total")
        p2["valor_desconto_scan"] = scan_record.get("valor_desconto")
        _csv_append(CSV_PRODUTOS, _PROD_FIELDS, p2)
    return scan_id


def _load_csv_scans() -> list[dict]:
    """Return all rows from scans.csv as list of dicts."""
    if not CSV_SCANS.exists():
        return []
    with CSV_SCANS.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _load_csv_produtos(scan_id: str | None = None) -> list[dict]:
    """Return rows from produtos.csv, optionally filtered by scan_id."""
    if not CSV_PRODUTOS.exists():
        return []
    with CSV_PRODUTOS.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if scan_id:
        rows = [r for r in rows if r.get("scan_id") == scan_id]
    return rows


def _is_duplicate_nf(chave_acesso: str | None) -> str | None:
    """Return existing scan_id if chave_acesso already in DB, else None."""
    if not chave_acesso:
        return None
    clean = chave_acesso.replace(" ", "").strip()
    if not clean or not CSV_SCANS.exists():
        return None
    with CSV_SCANS.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            ex = (row.get("chave_acesso") or "").replace(" ", "").strip()
            if ex and ex == clean:
                return row.get("scan_id", "?")
    return None


# ──────────────────────────────────────────────
# Infographic brand constants
# ──────────────────────────────────────────────
_BOT_LABEL  = "Notas Master Bot"
_BOT_HANDLE = "@notas_master_bot"
_C_BG      = "#0f172a"
_C_CARD    = "#1e293b"
_C_HDR     = "#1d4ed8"
_C_ACC     = "#38bdf8"
_C_TXT     = "#f1f5f9"
_C_MUTED   = "#94a3b8"
_C_GRN     = "#4ade80"
_C_YEL     = "#fbbf24"
_PALETTE   = ["#38bdf8","#818cf8","#f472b6","#34d399",
              "#fb923c","#facc15","#60a5fa","#a78bfa","#2dd4bf"]


# ─ Detect emoji-capable font for matplotlib (Windows: Segoe UI Emoji) ───────────────────
_FONT_FAMILIES = ["Segoe UI Emoji", "Segoe UI", "Apple Color Emoji",
                  "Noto Color Emoji", "Arial", "DejaVu Sans"]
_AVAIL_FONTS = {f.name for f in _fm.fontManager.ttflist}
_CHART_FONT  = next((f for f in _FONT_FAMILIES if f in _AVAIL_FONTS), "DejaVu Sans")


def _logo_arr():
    """Return logo as RGBA numpy array (resized), or None if unavailable."""
    try:
        from PIL import Image as _PIL_Image
        img = _PIL_Image.open(LOGO_PATH).convert("RGBA")
        h   = 110          # ← bigger logo
        w   = int(img.width * h / img.height)
        img = img.resize((w, h), _PIL_Image.LANCZOS)
        return np.array(img)
    except Exception:
        return None


def _put_logo(ax, x=0.006, y=0.08, w=0.065, h=0.84):
    """Embed bot logo as inset inside 'ax' (axes-fraction coords)."""
    arr = _logo_arr()
    if arr is None:
        return
    axins = ax.inset_axes([x, y, w, h])
    axins.imshow(arr)
    axins.axis("off")


def _brl_s(v, default="—") -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",","X").replace(".",",").replace("X",".")
    except (TypeError, ValueError):
        return default


def _styled_ax(ax, title: str = "", title_size: int = 10, bg: str = "") -> None:
    ax.set_facecolor(bg or _C_CARD)
    for sp in ax.spines.values():
        sp.set_edgecolor("#334155"); sp.set_linewidth(0.7)
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    if title:
        ax.text(0.02, 0.97, title, transform=ax.transAxes,
                fontsize=title_size, fontweight="bold", color=_C_ACC, va="top")


def _render_nf_image(scan_record: dict, scrape: dict, nf_meta: dict) -> bytes:
    """Return PNG bytes of a Power BI-style dark infographic for one NF.
    Figure height grows dynamically with product count. DPI=150 for crisp output."""
    plt.rcParams["font.family"] = _CHART_FONT
    plt.rcParams["axes.unicode_minus"] = False

    DPI       = 150
    ROW_H_IN  = 0.22   # inches per product row
    HDR_H_IN  = 0.52   # header bar
    INFO_H_IN = 3.60   # emitente / valores panel (fixed)
    PROD_MIN  = 1.00   # min height for products panel
    FTR_H_IN  = 0.38   # footer bar

    prods = scrape.get("produtos", [])
    n_prod = len(prods)

    prod_h_in  = max(PROD_MIN, 0.55 + n_prod * ROW_H_IN)   # header row + data rows
    total_h_in = HDR_H_IN + INFO_H_IN + prod_h_in + FTR_H_IN + 0.20  # +pad

    fig = plt.figure(figsize=(13, total_h_in), facecolor=_C_BG, dpi=DPI)
    gs  = GridSpec(
        4, 2, figure=fig,
        height_ratios=[HDR_H_IN, INFO_H_IN, prod_h_in, FTR_H_IN],
        hspace=0.06, wspace=0.08,
        left=0.015, right=0.985, top=0.99, bottom=0.01,
    )

    # ─ helper ──────────────────────────────────────────────
    def _t(ax, x, y, txt, fs=9, color=_C_TXT, bold=False, ha="left"):
        ax.text(x, y, txt, transform=ax.transAxes, fontsize=fs, color=color,
                fontweight="bold" if bold else "normal", va="top", ha=ha,
                clip_on=True)

    def _divider(ax, y_frac):
        ax.plot([0.01, 0.99], [y_frac, y_frac], color="#334155",
                linewidth=0.6, transform=ax.transAxes, clip_on=False)

    # ─ Header ──────────────────────────────────────────────
    ax_h = fig.add_subplot(gs[0, :])
    ax_h.set_facecolor(_C_HDR)
    for sp in ax_h.spines.values(): sp.set_visible(False)
    ax_h.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    modelo  = nf_meta.get("modelo") or "NFC-e"
    scan_id = scan_record.get("scan_id", "")
    _put_logo(ax_h, x=0.006, y=0.06, w=0.072, h=0.88)   # ← bigger logo
    ax_h.text(0.092, 0.62, _BOT_LABEL,
              transform=ax_h.transAxes, fontsize=11, fontweight="bold",
              color=_C_TXT, va="center")
    ax_h.text(0.092, 0.28, _BOT_HANDLE,
              transform=ax_h.transAxes, fontsize=8, color=_C_ACC, va="center")
    ax_h.text(0.50, 0.50, f"NOTA FISCAL ELETRONICA  [{modelo}]",
              transform=ax_h.transAxes, fontsize=14, fontweight="bold",
              color=_C_TXT, va="center", ha="center")
    ax_h.text(0.99, 0.50, f"Scan #{scan_id}",
              transform=ax_h.transAxes, fontsize=11, color=_C_ACC,
              va="center", ha="right")

    # ─ Left panel: Emitente + NF meta ──────────────────────
    ax_l = fig.add_subplot(gs[1, 0])
    _styled_ax(ax_l)
    nome  = (scan_record.get("emitente_nome") or nf_meta.get("cnpj_emit") or "—")[:50]
    cnpj  = scan_record.get("cnpj_emit") or "—"
    lid   = scan_record.get("loja_id") or "—"
    addr  = (scan_record.get("emitente_endereco") or "—")[:56]
    uf    = nf_meta.get("uf") or scan_record.get("uf") or "—"
    num   = scan_record.get("numero_nf") or "—"
    ser   = scan_record.get("serie") or "—"
    emi   = scan_record.get("data_emissao") or scrape.get("data_emissao_page") or "—"
    dhp   = scan_record.get("data_hora_page") or "—"
    proto = scan_record.get("numero_protocolo") or "—"
    if len(proto) > 28: proto = proto[:26] + "…"

    _t(ax_l, 0.03, 0.97, "[ EMITENTE ]", fs=9, color=_C_ACC, bold=True)
    rows_l = [("Nome", nome), ("CNPJ", cnpj), ("Loja ID", lid),
              ("Endereco", addr), ("UF", uf)]
    y = 0.89
    for k, v in rows_l:
        _t(ax_l, 0.03, y, k,  fs=7.5, color=_C_MUTED, bold=True)
        _t(ax_l, 0.27, y, v,  fs=8.2)
        y -= 0.092
    _divider(ax_l, 0.44)
    _t(ax_l, 0.03, 0.42, "[ NOTA FISCAL ]", fs=9, color=_C_ACC, bold=True)
    rows_nf = [("Numero", num), ("Serie", ser),
               ("Emissao", emi), ("Data/Hora", dhp), ("Protocolo", proto)]
    y = 0.35
    for k, v in rows_nf:
        _t(ax_l, 0.03, y, k,  fs=7.5, color=_C_MUTED, bold=True)
        _t(ax_l, 0.27, y, v,  fs=8.2)
        y -= 0.080

    # ─ Right panel: Values + Payment ───────────────────────
    ax_r = fig.add_subplot(gs[1, 1])
    _styled_ax(ax_r)
    vt  = _brl_s(scan_record.get("valor_total"))
    vd  = _brl_s(scan_record.get("valor_desconto"))
    vp  = _brl_s(scan_record.get("valor_pagar"))
    pag = scan_record.get("forma_pagamento") or "—"
    con = scan_record.get("consumidor") or "Nao identificado"
    amb = nf_meta.get("ambiente") or "—"

    _t(ax_r, 0.03, 0.97, "[ VALORES ]", fs=9, color=_C_ACC, bold=True)
    _t(ax_r, 0.50, 0.88, vt,          fs=24, color=_C_GRN, bold=True, ha="center")
    _t(ax_r, 0.50, 0.72, "TOTAL DA NF", fs=8, color=_C_MUTED, ha="center")
    rows_v = [("Desconto", vd, _C_YEL), ("A Pagar", vp, _C_TXT), ("Ambiente", amb, _C_MUTED)]
    y = 0.63
    for k, v, vc in rows_v:
        _t(ax_r, 0.03, y, k,  fs=7.5, color=_C_MUTED, bold=True)
        _t(ax_r, 0.36, y, v,  fs=9,   color=vc)
        y -= 0.095
    _divider(ax_r, 0.36)
    _t(ax_r, 0.03, 0.34, "[ PAGAMENTO ]", fs=9, color=_C_ACC, bold=True)
    pag_parts = pag.split(" | ")
    y = 0.27
    for part in pag_parts[:5]:
        _t(ax_r, 0.03, y, part[:52], fs=8.2)
        y -= 0.075
    _t(ax_r, 0.03, max(y - 0.02, 0.04), f"Consumidor: {con[:50]}", fs=7.8, color=_C_MUTED)

    # ─ Products table (dynamic height) ─────────────────────
    ax_p = fig.add_subplot(gs[2, :])
    _styled_ax(ax_p)
    ax_p.set_xlim(0, 1)
    ax_p.set_ylim(0, 1)

    # Column x-positions and widths (fractional page units)
    # Desc(0-51%), Cod(52-63%), Qtd(64-73%), Unit(74-86%), Total(87-99%)
    C = [0.010, 0.520, 0.640, 0.745, 0.875]
    H = ["Descricao", "Cod.", "Qtd", "Unit.", "Total"]

    # Compute y step so all rows fit inside [0.05 .. 0.90] with header at 0.93
    available   = 0.88          # from 0.05 to 0.93
    max_display = n_prod        # show ALL products
    row_step    = min(0.090, available / (max_display + 1)) if max_display else 0.10
    fs_row      = max(6.5, min(8.5, 8.5 - max(0, (n_prod - 8)) * 0.18))

    _t(ax_p, 0.01, 0.97,
       f"[ PRODUTOS ]   {n_prod} item{'s' if n_prod != 1 else ''}",
       fs=9, color=_C_ACC, bold=True)

    y_h = 0.91
    for cx, ht in zip(C, H):
        _t(ax_p, cx, y_h, ht, fs=7.5, color=_C_MUTED, bold=True)

    # Thin line under header
    ax_p.plot([0.005, 0.995], [y_h - row_step * 0.55, y_h - row_step * 0.55],
              color="#334155", linewidth=0.5,
              transform=ax_p.transAxes, clip_on=False)

    y_row = y_h - row_step
    for idx, prod in enumerate(prods):
        bg_col = "#253048" if idx % 2 == 0 else _C_CARD
        rect = mpatches.FancyBboxPatch(
            (0.002, y_row - row_step * 0.75), 0.996, row_step * 0.90,
            boxstyle="square,pad=0", facecolor=bg_col, edgecolor="none",
            transform=ax_p.transAxes, clip_on=True, zorder=0,
        )
        ax_p.add_patch(rect)
        desc  = (prod.get("descricao") or "")[:46]
        cod   = str(prod.get("codigo") or "")[:14]
        qtd   = prod.get("quantidade")
        qtd_s = f"{qtd:g} {prod.get('unidade','')}" .strip() if qtd is not None else "—"
        vu    = _brl_s(prod.get("valor_unitario"))
        vt2   = _brl_s(prod.get("valor_total"))
        for cx, v in zip(C, [desc, cod, qtd_s, vu, vt2]):
            _t(ax_p, cx, y_row, v, fs=fs_row, color=_C_TXT)
        y_row -= row_step

    # ─ Footer ──────────────────────────────────────────────
    ax_f = fig.add_subplot(gs[3, :])
    ax_f.set_facecolor("#0a0e1a")
    for sp in ax_f.spines.values(): sp.set_visible(False)
    ax_f.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    _put_logo(ax_f, x=0.006, y=0.05, w=0.038, h=0.90)
    ax_f.text(0.052, 0.50, f"{_BOT_LABEL}  ·  {_BOT_HANDLE}  |  Leitor NFC-e BR",
              transform=ax_f.transAxes, fontsize=8, color=_C_MUTED, va="center")
    ax_f.text(0.99, 0.50, f"Gerado em {ts}",
              transform=ax_f.transAxes, fontsize=8, color=_C_MUTED,
              va="center", ha="right")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=_C_BG, bbox_inches="tight", dpi=DPI)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _render_resume_image(
    scans: list,
    emit_sorted: list,
    pag_sorted: list,
    prods_sorted: list,
    periodo_label: str,
    vt_sum: float,
    vd_sum: float,
    vp_sum: float,
) -> bytes:
    """Return PNG bytes of a Power BI-style dark infographic for a period summary."""
    plt.rcParams["font.family"] = _CHART_FONT
    plt.rcParams["axes.unicode_minus"] = False

    DPI     = 150
    n_prods = min(len([p for p in prods_sorted if (p.get("total_val") or 0) > 0]), 10)
    n_stores= min(len(emit_sorted), 6)

    # Dynamic height: header + KPI + (products+pie) + stores + footer
    PROD_ROW_H  = 0.30   # inches per product bar row
    STORE_ROW_H = 0.38   # inches per store row
    HDR_H   = 0.55
    KPI_H   = 1.30
    MID_H   = max(2.80, 0.50 + n_prods * PROD_ROW_H)
    ST_H    = max(1.60, 0.50 + n_stores * STORE_ROW_H)
    FTR_H   = 0.42
    PAD     = 0.25
    total_h = HDR_H + KPI_H + MID_H + ST_H + FTR_H + PAD

    fig = plt.figure(figsize=(14, total_h), facecolor=_C_BG, dpi=DPI)
    gs  = GridSpec(5, 4, figure=fig,
                   height_ratios=[HDR_H, KPI_H, MID_H, ST_H, FTR_H],
                   hspace=0.10, wspace=0.14,
                   left=0.015, right=0.985, top=0.995, bottom=0.005)

    def _rt(ax, x, y, txt, fs=9, color=_C_TXT, bold=False, ha="left", va="top"):
        ax.text(x, y, txt, transform=ax.transAxes, fontsize=fs,
                color=color, fontweight="bold" if bold else "normal",
                ha=ha, va=va, clip_on=True)

    # ─ Header ──────────────────────────────────────────
    ax_h = fig.add_subplot(gs[0, :])
    ax_h.set_facecolor(_C_HDR)
    for sp in ax_h.spines.values(): sp.set_visible(False)
    ax_h.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    _put_logo(ax_h, x=0.006, y=0.06, w=0.055, h=0.88)
    ax_h.text(0.076, 0.62, _BOT_LABEL,
              transform=ax_h.transAxes, fontsize=11, fontweight="bold", color=_C_TXT, va="center")
    ax_h.text(0.076, 0.26, _BOT_HANDLE,
              transform=ax_h.transAxes, fontsize=8, color=_C_ACC, va="center")
    ax_h.text(0.50, 0.50, f"RESUMO DE COMPRAS  \u2014  {periodo_label.upper()}",
              transform=ax_h.transAxes, fontsize=14, fontweight="bold",
              color=_C_TXT, va="center", ha="center")
    ax_h.text(0.99, 0.50, f"{len(scans)} NF{'s' if len(scans)!=1 else ''}",
              transform=ax_h.transAxes, fontsize=11, color=_C_ACC, va="center", ha="right")

    # ─ KPI cards (5 full-width) ───────────────────────
    ax_kpi = fig.add_subplot(gs[1, :])
    ax_kpi.set_facecolor(_C_BG)
    for sp in ax_kpi.spines.values(): sp.set_visible(False)
    ax_kpi.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ax_kpi.set_xlim(0, 1); ax_kpi.set_ylim(0, 1)
    n_users = len({s.get("user_id") for s in scans})
    kpis = [
        ("NFs Escaneadas", str(len(scans)),  _C_ACC),
        ("Total Gasto",    _brl_s(vt_sum),   _C_TXT),
        ("Descontos",      _brl_s(vd_sum),   _C_YEL),
        ("Total Pago",     _brl_s(vp_sum),   _C_GRN),
        ("Usuarios",       str(n_users),     _C_ACC),
    ]
    cw = 1.0 / len(kpis)
    for i, (label, val, vc) in enumerate(kpis):
        cx = i * cw + 0.005
        rect = mpatches.FancyBboxPatch(
            (cx, 0.05), cw - 0.010, 0.90,
            boxstyle="round,pad=0.01",
            facecolor=_C_CARD, edgecolor="#334155", linewidth=0.8,
            transform=ax_kpi.transAxes, clip_on=False,
        )
        ax_kpi.add_patch(rect)
        mid = cx + (cw - 0.010) / 2
        ax_kpi.text(mid, 0.90, label, transform=ax_kpi.transAxes,
                    fontsize=8.5, color=_C_MUTED, ha="center", va="top")
        vfs = 15 if len(val) <= 9 else 11
        ax_kpi.text(mid, 0.60, val, transform=ax_kpi.transAxes,
                    fontsize=vfs, fontweight="bold", color=vc, ha="center", va="top")
        # Thin bottom-border accent
        ax_kpi.plot([cx, cx + cw - 0.010], [0.06, 0.06],
                    transform=ax_kpi.transAxes, color=vc, linewidth=2, clip_on=False)

    # ─ Products bar — left 3 cols ─────────────────────
    ax_bar = fig.add_subplot(gs[2, :3])
    _styled_ax(ax_bar, "TOP PRODUTOS  (valor total)", bg=_C_CARD)
    top_p = [p for p in prods_sorted if (p.get("total_val") or 0) > 0][:10]
    if top_p:
        labels_b = [
            (p.get("descricao") or "?")[:30]
            + (("/".join(sorted(p.get("lojas", set())))))[:8].join([" [","]"] if p.get("lojas") else ["",""])
            for p in top_p
        ]
        vals_b   = [p.get("total_val", 0) for p in top_p]
        colors_b = [_PALETTE[i % len(_PALETTE)] for i in range(len(top_p))]
        bars = ax_bar.barh(range(len(top_p)), vals_b, color=colors_b,
                           edgecolor="none", height=0.68)
        ax_bar.set_yticks(range(len(top_p)))
        ax_bar.set_yticklabels(labels_b, fontsize=8.5, color=_C_TXT)
        ax_bar.invert_yaxis()
        ax_bar.xaxis.set_visible(False)
        ax_bar.set_facecolor(_C_CARD)
        mx = max(vals_b) or 1
        for bar, val in zip(bars, vals_b):
            ax_bar.text(bar.get_width() + mx * 0.008,
                        bar.get_y() + bar.get_height() / 2,
                        _brl_s(val), va="center", fontsize=8, color=_C_TXT)
        ax_bar.set_xlim(0, mx * 1.22)
    else:
        ax_bar.text(0.5, 0.5, "(sem dados)", ha="center", va="center",
                    color=_C_MUTED, fontsize=10, transform=ax_bar.transAxes)

    # ─ Payment pie — right col ────────────────────────
    ax_pie = fig.add_subplot(gs[2, 3])
    _styled_ax(ax_pie, "PAGAMENTOS", bg=_C_CARD)
    pag_items = [(k, v["total"]) for k, v in pag_sorted if v.get("total", 0) > 0][:6]
    if pag_items:
        labels_pie = [k[:20] for k, _ in pag_items]
        sizes      = [v for _, v in pag_items]
        cols_pie   = [_PALETTE[i % len(_PALETTE)] for i in range(len(pag_items))]
        wedges, _, autotexts = ax_pie.pie(
            sizes, labels=None, colors=cols_pie,
            autopct="%1.0f%%", startangle=90, pctdistance=0.74,
            wedgeprops=dict(edgecolor=_C_CARD, linewidth=1.5),
        )
        for at in autotexts:
            at.set_fontsize(7.5); at.set_color(_C_TXT)
        ax_pie.legend(wedges, labels_pie, loc="lower center", fontsize=7,
                      facecolor=_C_CARD, edgecolor="#334155", labelcolor=_C_TXT,
                      bbox_to_anchor=(0.5, -0.22), ncol=2)
    else:
        ax_pie.text(0.5, 0.5, "(sem dados)", ha="center", va="center",
                    color=_C_MUTED, fontsize=10, transform=ax_pie.transAxes)

    # ─ Stores full-width bar ──────────────────────────
    ax_st = fig.add_subplot(gs[3, :])
    _styled_ax(ax_st, "LOJAS", bg=_C_CARD)
    stores = emit_sorted[:6]
    if stores:
        s_vals   = [e.get("vt", 0) or e.get("cnt", 0) for e in stores]
        s_labels = [
            (e.get("nome") or "?")[:35]
            + (f" [{e['loja_id']}]" if e.get("loja_id") else "")
            + f"  —  {e.get('cnt',0)}x"
            for e in stores
        ]
        s_colors = [_PALETTE[i % len(_PALETTE)] for i in range(len(stores))]
        sbars = ax_st.barh(range(len(stores)), s_vals, color=s_colors,
                           edgecolor="none", height=0.55)
        ax_st.set_yticks(range(len(stores)))
        ax_st.set_yticklabels(s_labels, fontsize=9, color=_C_TXT)
        ax_st.invert_yaxis()
        ax_st.xaxis.set_visible(False)
        ax_st.set_facecolor(_C_CARD)
        mx = max(s_vals) or 1
        for bar, e in zip(sbars, stores):
            lbl = _brl_s(e.get("vt", 0)) if e.get("vt", 0) > 0 else f"{e.get('cnt',0)}x"
            ax_st.text(bar.get_width() + mx * 0.008,
                       bar.get_y() + bar.get_height() / 2,
                       lbl, va="center", fontsize=8.5, color=_C_TXT)
        ax_st.set_xlim(0, mx * 1.20)
    else:
        ax_st.text(0.5, 0.5, "(sem dados)", ha="center", va="center",
                   color=_C_MUTED, fontsize=10, transform=ax_st.transAxes)

    # ─ Footer ─────────────────────────────────────────
    ax_f = fig.add_subplot(gs[4, :])
    ax_f.set_facecolor("#0a0e1a")
    for sp in ax_f.spines.values(): sp.set_visible(False)
    ax_f.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    _put_logo(ax_f, x=0.005, y=0.06, w=0.028, h=0.88)
    ax_f.text(0.042, 0.50, f"{_BOT_LABEL}  ·  {_BOT_HANDLE}  |  Leitor NFC-e BR",
              transform=ax_f.transAxes, fontsize=8, color=_C_MUTED, va="center")
    ax_f.text(0.99, 0.50, f"Gerado em {ts}",
              transform=ax_f.transAxes, fontsize=8, color=_C_MUTED, va="center", ha="right")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=_C_BG, bbox_inches="tight", dpi=DPI)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ══════════════════════════════════════════════
# PDF generators  (ReportLab / Platypus)
# ══════════════════════════════════════════════
_PDF_BLUE      = _rl_colors.HexColor("#1d4ed8")
_PDF_ACCENT    = _rl_colors.HexColor("#1e40af")
_PDF_ACC_LIGHT = _rl_colors.HexColor("#3b82f6")
_PDF_CARD      = _rl_colors.HexColor("#f1f5f9")
_PDF_TXT       = _rl_colors.HexColor("#0f172a")
_PDF_MUTED     = _rl_colors.HexColor("#475569")
_PDF_GRN       = _rl_colors.HexColor("#15803d")
_PDF_LINE      = _rl_colors.HexColor("#cbd5e1")
_PDF_WHITE     = _rl_colors.white
_PDF_DARK      = _rl_colors.HexColor("#0f172a")
_PDF_AMBER     = _rl_colors.HexColor("#b45309")


def _brl_pdf(v) -> str:
    """Format a value as BRL currency string."""
    try:
        f = float(str(v).replace(",", ".").strip()) if v else 0.0
        return f"R$ {f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"


def _pdf_logo_rl(height_pt: float = 36):
    """Return a ReportLab Image of the bot logo, or None."""
    try:
        from PIL import Image as _PILImg
        with _PILImg.open(LOGO_PATH) as _im:
            _w, _h = _im.size
        aspect = _w / _h
        return _rl_Img(str(LOGO_PATH), width=height_pt * aspect, height=height_pt)
    except Exception:
        return None


def _pdf_page_cb(title: str, subtitle: str = ""):
    """Return a canvas callback that draws header + footer on every PDF page."""
    _pw, _ph = _RL_A4
    _hdr_h   = 72   # taller header to fit bigger logo
    _acc_h   = 4    # accent stripe below header
    _ftr_h   = 24
    _C_HDR2  = _rl_colors.HexColor("#1e3a8a")   # darker band for logo zone
    _C_HLBL  = _rl_colors.HexColor("#bfdbfe")   # light-blue label
    _C_HSUB  = _rl_colors.HexColor("#93c5fd")   # slightly dimmer subtitle
    _C_ACC   = _rl_colors.HexColor("#38bdf8")   # cyan accent stripe

    def _draw(canvas, doc):
        canvas.saveState()

        _BT = _ph - _hdr_h   # y of header bottom edge

        # ─ Header: main blue band ───────────────────────────────────
        canvas.setFillColor(_PDF_BLUE)
        canvas.rect(0, _BT, _pw, _hdr_h, fill=1, stroke=0)

        # ─ Identity zone (darker left block) ────────────────────────
        # Width is dynamic: logo width + 8px padding + text + 14px right margin
        _LOGO_H  = 52
        _logo_img = None
        _logo_w   = 0
        try:
            _logo_img = _pdf_logo_rl(height_pt=_LOGO_H)
            if _logo_img:
                _logo_w = _logo_img.drawWidth
        except Exception:
            pass

        _id_w = int(_logo_w) + 8 + 115   # logo + gap + text column
        canvas.setFillColor(_C_HDR2)
        canvas.rect(0, _BT, _id_w, _hdr_h, fill=1, stroke=0)

        # Draw logo — vertically centered
        _lx = 8
        if _logo_img:
            _logo_y = _BT + (_hdr_h - _LOGO_H) / 2
            _logo_img.drawOn(canvas, _lx, _logo_y)
        _tx = _lx + _logo_w + 8   # x where text starts

        # Three text rows — evenly spaced inside header
        _y1 = _BT + 50   # Bot name   (top)
        _y2 = _BT + 34   # Handle     (middle)
        _y3 = _BT + 20   # Sub-label  (bottom)

        canvas.setFont("Helvetica-Bold", 11)
        canvas.setFillColor(_PDF_WHITE)
        canvas.drawString(_tx, _y1, _BOT_LABEL)

        canvas.setFont("Helvetica", 8.5)
        canvas.setFillColor(_C_HLBL)
        canvas.drawString(_tx, _y2, _BOT_HANDLE)

        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(_C_HSUB)
        canvas.drawString(_tx, _y3, "Leitor NFC-e BR")

        # Thin vertical separator
        canvas.setStrokeColor(_rl_colors.HexColor("#3b82f6"))
        canvas.setLineWidth(0.8)
        canvas.line(_id_w, _BT + 6, _id_w, _BT + _hdr_h - 6)

        # ─ Title — centered in right zone ───────────────────────────
        _cx = _id_w + (_pw - _id_w) / 2
        canvas.setFont("Helvetica-Bold", 13)
        canvas.setFillColor(_PDF_WHITE)
        canvas.drawCentredString(_cx, _BT + 44, title)
        if subtitle:
            canvas.setFont("Helvetica", 8.5)
            canvas.setFillColor(_C_HLBL)
            canvas.drawCentredString(_cx, _BT + 28, subtitle)

        # Page number — bottom-right of header
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(_rl_colors.HexColor("#e2e8f0"))
        canvas.drawRightString(_pw - 10, _BT + 12, f"Pág. {doc.page}")

        # Cyan accent stripe below header
        canvas.setFillColor(_C_ACC)
        canvas.rect(0, _ph - _hdr_h - _acc_h, _pw, _acc_h, fill=1, stroke=0)

        # ─ Footer bar ─────────────────────────────────────────────
        canvas.setFillColor(_PDF_DARK)
        canvas.rect(0, 0, _pw, _ftr_h, fill=1, stroke=0)
        # Tiny accent line at very top of footer
        canvas.setFillColor(_C_ACC)
        canvas.rect(0, _ftr_h, _pw, 2, fill=1, stroke=0)
        _ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        canvas.setFont("Helvetica-Bold", 7)
        canvas.setFillColor(_rl_colors.HexColor("#cbd5e1"))
        canvas.drawString(10, 9, f"{_BOT_LABEL}  ·  {_BOT_HANDLE}  ·  Leitor NFC-e BR")
        canvas.setFont("Helvetica", 6.5)
        canvas.setFillColor(_rl_colors.HexColor("#64748b"))
        canvas.drawString(10, 2, "Documento gerado automaticamente pelo bot Telegram")
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(_rl_colors.HexColor("#94a3b8"))
        canvas.drawRightString(_pw - 10, 9, f"Gerado em {_ts}")
        canvas.restoreState()

    return _draw


def _build_nf_pdf(scan_record: dict, scrape: dict, nf_meta: dict,
                  qr_png_bytes: bytes | None = None) -> bytes:
    """Build and return a PDF for one NF scan.  Auto-paginates product list."""
    buf = io.BytesIO()
    _pw, _ph = _RL_A4
    _usable  = _pw - 2 * 1.2 * _RL_CM

    doc = _rl_Doc(
        buf, pagesize=_RL_A4,
        leftMargin=1.2*_RL_CM, rightMargin=1.2*_RL_CM,
        topMargin=82, bottomMargin=32,
        title="Nota Fiscal Eletrônica", author=f"{_BOT_LABEL} ({_BOT_HANDLE})",
    )

    _ss = _rl_styles()
    def _sty(n, **kw):
        return _rl_ParagraphStyle(n, parent=_ss["Normal"], **kw)

    S_SEC   = _sty("s",  fontSize=9,  textColor=_PDF_ACC_LIGHT, fontName="Helvetica-Bold",
                   spaceAfter=3, spaceBefore=8)
    S_KK    = _sty("kk", fontSize=8,  textColor=_PDF_MUTED,     fontName="Helvetica-Bold", leading=12)
    S_KV    = _sty("kv", fontSize=8,  textColor=_PDF_TXT,       fontName="Helvetica",      leading=12)
    S_TH    = _sty("th", fontSize=8,  textColor=_PDF_WHITE,     fontName="Helvetica-Bold", alignment=_TA_CENTER)
    S_TD    = _sty("td", fontSize=7.5,textColor=_PDF_TXT,       fontName="Helvetica",      leading=10)
    S_TD_C  = _sty("tc", fontSize=7.5,textColor=_PDF_TXT,       fontName="Helvetica",      alignment=_TA_CENTER, leading=10)
    S_TD_R  = _sty("tr", fontSize=7.5,textColor=_PDF_TXT,       fontName="Helvetica",      alignment=_TA_RIGHT,  leading=10)
    S_CRED  = _sty("cr", fontSize=7.5,textColor=_PDF_MUTED,     fontName="Helvetica-Oblique",
                   alignment=_TA_CENTER, spaceAfter=4)

    prods   = scrape.get("produtos", [])
    n_prod  = len(prods)
    nome    = (scan_record.get("emitente_nome") or nf_meta.get("cnpj_emit") or "—")[:60]
    cnpj    = scan_record.get("cnpj_emit") or "—"
    lid     = scan_record.get("loja_id") or "—"
    addr    = (scan_record.get("emitente_endereco") or "—")[:80]
    uf      = nf_meta.get("uf") or scan_record.get("uf") or "—"
    num_nf  = scan_record.get("numero_nf") or "—"
    ser     = scan_record.get("serie") or "—"
    emi     = scan_record.get("data_emissao") or scrape.get("data_emissao_page") or "—"
    dhp     = scan_record.get("data_hora_page") or "—"
    proto   = scan_record.get("numero_protocolo") or "—"
    modelo  = nf_meta.get("modelo") or "NFC-e"
    scan_id = scan_record.get("scan_id", "—")
    amb     = nf_meta.get("ambiente") or "—"
    vt_nf   = _brl_pdf(scan_record.get("valor_total"))
    vd_nf   = _brl_pdf(scan_record.get("valor_desconto"))
    vp_nf   = _brl_pdf(scan_record.get("valor_pagar"))
    pag_nf  = scan_record.get("forma_pagamento") or "—"
    con_nf  = (scan_record.get("consumidor") or "Não identificado")[:80]

    story = []

    # Credit line
    story.append(_rl_P(
        f"<i>Documento gerado por <b>{_BOT_LABEL}</b> ({_BOT_HANDLE}) · Leitor NFC-e BR · Scan #{scan_id}</i>",
        S_CRED))
    story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=6))

    # ── Two-column info panel ─────────────────────────────────────────────
    col_w = _usable / 2 - 3

    def _info_tbl(rows: list[tuple]):
        """Build a key-value inner table from [(key, value), ...] with SECTION markers."""
        td, ts_cmds = [], [
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
            ("RIGHTPADDING",  (0,0), (-1,-1), 4),
            ("TOPPADDING",    (0,0), (-1,-1), 1),
            ("BOTTOMPADDING", (0,0), (-1,-1), 1),
        ]
        for i, (k, v) in enumerate(rows):
            if k and v is None:   # section header
                td.append([_rl_P(f"<b>{k}</b>",
                    _sty(f"sh{i}", fontSize=8, textColor=_PDF_ACC_LIGHT, fontName="Helvetica-Bold")), ""])
                ts_cmds += [("SPAN", (0,i),(1,i)), ("TOPPADDING",(0,i),(1,i),5),
                            ("LINEABOVE",(0,i),(-1,i),0.4,_PDF_LINE)]
            else:
                td.append([_rl_P(f"<b>{k}</b>", S_KK) if k else "",
                           _rl_P(str(v) if v is not None else "", S_KV)])
        return _rl_Table(td, colWidths=[col_w*0.36, col_w*0.64], style=_rl_TS(ts_cmds))

    left_rows: list[tuple] = [
        ("EMITENTE",  None),
        ("Nome",      nome),
        ("CNPJ",      cnpj),
        ("Loja ID",   lid),
        ("Endereço",  addr),
        ("UF",        uf),
        ("",          ""),
        ("NOTA FISCAL", None),
        ("Número",    num_nf),
        ("Série",     ser),
        ("Emissão",   emi),
        ("Data/Hora", dhp),
        ("Protocolo", str(proto)[:44]),
        ("Modelo",    modelo),
        ("Ambiente",  amb),
    ]
    right_rows: list[tuple] = [
        ("VALORES",      None),
        ("Total da NF",  vt_nf),
        ("Desconto",     vd_nf),
        ("A Pagar",      vp_nf),
        ("",             ""),
        ("PAGAMENTO",    None),
    ]
    for _part in pag_nf.split(" | "):
        right_rows.append(("", _part.strip()))
    right_rows += [
        ("",           ""),
        ("CONSUMIDOR", None),
        ("",           con_nf),
    ]
    # Pad to same height
    while len(left_rows) < len(right_rows):  left_rows.append(("", ""))
    while len(right_rows) < len(left_rows): right_rows.append(("", ""))

    info_tbl = _rl_Table(
        [[_info_tbl(left_rows), _info_tbl(right_rows)]],
        colWidths=[col_w, col_w],
        style=_rl_TS([
            ("VALIGN",      (0,0), (-1,-1), "TOP"),
            ("LEFTPADDING", (0,0), (-1,-1), 4),
            ("RIGHTPADDING",(0,0), (-1,-1), 4),
            ("BOX",         (0,0), (0,0), 0.5, _PDF_LINE),
            ("BOX",         (1,0), (1,0), 0.5, _PDF_LINE),
            ("BACKGROUND",  (0,0), (0,0), _PDF_CARD),
        ]),
    )
    story.append(info_tbl)
    story.append(_rl_Spacer(1, 8))

    # ── Products table ─────────────────────────────────────────────────────
    story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=3))
    story.append(_rl_P(f"PRODUTOS  ({n_prod} item{'s' if n_prod != 1 else ''})", S_SEC))

    pr_cw = [_usable * r for r in [0.42, 0.12, 0.10, 0.08, 0.14, 0.14]]
    prod_data = [[
        _rl_P(t, S_TH) for t in ["Descrição", "Código", "Qtd", "Un.", "Unit.", "Total"]
    ]]
    for prod in prods:
        desc  = str(prod.get("descricao") or "")[:56]
        cod   = str(prod.get("codigo")    or "")[:14]
        qtd   = prod.get("quantidade")
        qtd_s = f"{qtd:g}" if qtd is not None else "—"
        un    = str(prod.get("unidade") or "")[:5]
        vu    = _brl_pdf(prod.get("valor_unitario"))
        vt2   = _brl_pdf(prod.get("valor_total"))
        prod_data.append([
            _rl_P(desc, S_TD),
            _rl_P(cod,  S_TD),
            _rl_P(qtd_s, S_TD_C),
            _rl_P(un,   S_TD_C),
            _rl_P(vu,   S_TD_R),
            _rl_P(vt2,  S_TD_R),
        ])

    story.append(_rl_Table(prod_data, colWidths=pr_cw, repeatRows=1, style=_rl_TS([
        ("BACKGROUND",    (0,0), (-1,0), _PDF_ACCENT),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [_rl_colors.white, _PDF_CARD]),
        ("GRID",          (0,0), (-1,-1), 0.3, _PDF_LINE),
        ("LEFTPADDING",   (0,0), (-1,-1), 4),
        ("RIGHTPADDING",  (0,0), (-1,-1), 4),
        ("TOPPADDING",    (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ])))

    # ── QR code image (optional) ───────────────────────────────────────────
    if qr_png_bytes:
        try:
            story.append(_rl_Spacer(1, 10))
            story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=3))
            story.append(_rl_P("QR CODE DA NF", S_SEC))
            _qr_img = _rl_Img(io.BytesIO(qr_png_bytes), width=4.5*_RL_CM, height=4.5*_RL_CM)
            _qr_img.hAlign = "LEFT"
            story.append(_qr_img)
        except Exception:
            pass

    doc.build(story,
              onFirstPage=_pdf_page_cb(f"NOTA FISCAL ELETRÔNICA  [{modelo}]",
                                       subtitle=f"Scan #{scan_id}  ·  {nome[:48]}"),
              onLaterPages=_pdf_page_cb(f"NOTA FISCAL ELETRÔNICA  [{modelo}]",
                                        subtitle=f"Scan #{scan_id}  ·  {nome[:48]}"))
    buf.seek(0)
    return buf.read()


def _build_resume_pdf(
    scans: list,
    emit_sorted: list,
    pag_sorted: list,
    prods_sorted: list,
    periodo_label: str,
    vt_sum: float,
    vd_sum: float,
    vp_sum: float,
) -> bytes:
    """Build and return a PDF for a period summary.  Auto-paginates all tables."""
    buf = io.BytesIO()
    _pw, _ph = _RL_A4
    _usable  = _pw - 2 * 1.2 * _RL_CM
    n_users  = len({s.get("user_id") for s in scans})

    doc = _rl_Doc(
        buf, pagesize=_RL_A4,
        leftMargin=1.2*_RL_CM, rightMargin=1.2*_RL_CM,
        topMargin=82, bottomMargin=32,
        title=f"Resumo de Compras — {periodo_label}", author=f"{_BOT_LABEL} ({_BOT_HANDLE})",
    )

    _ss = _rl_styles()
    def _sty(n, **kw):
        return _rl_ParagraphStyle(n, parent=_ss["Normal"], **kw)

    S_SEC  = _sty("rs",  fontSize=9,  textColor=_PDF_ACC_LIGHT, fontName="Helvetica-Bold",
                  spaceAfter=3, spaceBefore=10)
    S_TH   = _sty("rth", fontSize=8,  textColor=_PDF_WHITE,     fontName="Helvetica-Bold",
                  alignment=_TA_CENTER)
    S_TD   = _sty("rtd", fontSize=8,  textColor=_PDF_TXT,       fontName="Helvetica",      leading=11)
    S_TD_C = _sty("rtc", fontSize=8,  textColor=_PDF_TXT,       fontName="Helvetica",
                  alignment=_TA_CENTER, leading=11)
    S_TD_R = _sty("rtr", fontSize=8,  textColor=_PDF_TXT,       fontName="Helvetica",
                  alignment=_TA_RIGHT,  leading=11)
    S_CRED = _sty("rcr", fontSize=7.5,textColor=_PDF_MUTED,     fontName="Helvetica-Oblique",
                  alignment=_TA_CENTER, spaceAfter=4)

    story = []

    # Credit line
    story.append(_rl_P(
        f"<i>Documento gerado por <b>{_BOT_LABEL}</b> ({_BOT_HANDLE}) · Leitor NFC-e BR · Período: {periodo_label}</i>",
        S_CRED))
    story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=6))

    # ── KPI cards row ─────────────────────────────────────────────────────
    story.append(_rl_P("RESUMO DO PERÍODO", S_SEC))
    kpis = [
        ("NFs Escaneadas", str(len(scans)),  _PDF_ACC_LIGHT),
        ("Total Gasto",    _brl_pdf(vt_sum), _PDF_TXT),
        ("Descontos",      _brl_pdf(vd_sum), _PDF_AMBER),
        ("Total Pago",     _brl_pdf(vp_sum), _PDF_GRN),
        ("Usuários",       str(n_users),     _PDF_ACC_LIGHT),
    ]
    kw = _usable / len(kpis)
    kpi_cells = []
    for i, (label, val, vc) in enumerate(kpis):
        cell = _rl_Table(
            [[_rl_P(label, _sty(f"kl{i}", fontSize=7.5, textColor=_PDF_MUTED,
                                fontName="Helvetica", alignment=_TA_CENTER))],
             [_rl_P(val,   _sty(f"kv{i}", fontSize=14,  textColor=vc,
                                fontName="Helvetica-Bold", alignment=_TA_CENTER))]],
            style=_rl_TS([
                ("BACKGROUND",    (0,0), (-1,-1), _PDF_CARD),
                ("TOPPADDING",    (0,0), (-1,-1), 5),
                ("BOTTOMPADDING", (0,0), (-1,-1), 5),
                ("LINEBELOW",     (0,1), (-1,1),  2, vc),
                ("LEFTPADDING",   (0,0), (-1,-1), 3),
                ("RIGHTPADDING",  (0,0), (-1,-1), 3),
            ]),
        )
        kpi_cells.append(cell)
    story.append(_rl_Table([kpi_cells], colWidths=[kw]*len(kpis),
                            style=_rl_TS([("VALIGN",(0,0),(-1,-1),"TOP"),
                                          ("LEFTPADDING",(0,0),(-1,-1),3),
                                          ("RIGHTPADDING",(0,0),(-1,-1),3)])))
    story.append(_rl_Spacer(1, 10))

    # ── Stores table ──────────────────────────────────────────────────────
    if emit_sorted:
        story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=3))
        story.append(_rl_P("LOCAIS DE COMPRA", S_SEC))
        sh = [_rl_P(t, S_TH) for t in ["Estabelecimento", "ID", "Visitas", "Total gasto"]]
        st_data = [sh]
        for e in emit_sorted:
            st_data.append([
                _rl_P((e.get("nome") or "—")[:55], S_TD),
                _rl_P(e.get("loja_id") or "—",     S_TD_C),
                _rl_P(f"{e.get('cnt',0)}x",         S_TD_C),
                _rl_P(_brl_pdf(e.get("vt", 0)),     S_TD_R),
            ])
        story.append(_rl_Table(st_data, repeatRows=1,
                               colWidths=[_usable*r for r in [0.52,0.10,0.12,0.26]],
                               style=_rl_TS([
                                   ("BACKGROUND",    (0,0), (-1,0), _PDF_ACCENT),
                                   ("ROWBACKGROUNDS",(0,1), (-1,-1), [_rl_colors.white, _PDF_CARD]),
                                   ("GRID",          (0,0), (-1,-1), 0.3, _PDF_LINE),
                                   ("LEFTPADDING",   (0,0), (-1,-1), 4),
                                   ("RIGHTPADDING",  (0,0), (-1,-1), 4),
                                   ("TOPPADDING",    (0,0), (-1,-1), 3),
                                   ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                                   ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
                               ])))
        story.append(_rl_Spacer(1, 8))

    # ── Payment table ─────────────────────────────────────────────────────
    if pag_sorted:
        story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=3))
        story.append(_rl_P("FORMAS DE PAGAMENTO", S_SEC))
        ph = [_rl_P(t, S_TH) for t in ["Forma de Pagamento", "Ocorrências", "Total"]]
        pag_data = [ph]
        for forma, data in pag_sorted:
            if (data.get("total") or 0) <= 0:
                continue
            pag_data.append([
                _rl_P(str(forma)[:50],              S_TD),
                _rl_P(f"{data.get('cnt',0)}x",     S_TD_C),
                _rl_P(_brl_pdf(data.get("total",0)), S_TD_R),
            ])
        story.append(_rl_Table(pag_data, repeatRows=1,
                               colWidths=[_usable*r for r in [0.54,0.20,0.26]],
                               style=_rl_TS([
                                   ("BACKGROUND",    (0,0), (-1,0), _PDF_ACCENT),
                                   ("ROWBACKGROUNDS",(0,1), (-1,-1), [_rl_colors.white, _PDF_CARD]),
                                   ("GRID",          (0,0), (-1,-1), 0.3, _PDF_LINE),
                                   ("LEFTPADDING",   (0,0), (-1,-1), 4),
                                   ("RIGHTPADDING",  (0,0), (-1,-1), 4),
                                   ("TOPPADDING",    (0,0), (-1,-1), 3),
                                   ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                                   ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
                               ])))
        story.append(_rl_Spacer(1, 8))

    # ── Products table ────────────────────────────────────────────────────
    if prods_sorted:
        story.append(_rl_HR(width="100%", thickness=0.5, color=_PDF_LINE, spaceAfter=3))
        story.append(_rl_P(
            f"PRODUTOS COMPRADOS NO PERÍODO  ({len(prods_sorted)} {'item' if len(prods_sorted)==1 else 'itens'})",
            S_SEC))
        pr_h = [_rl_P(t, S_TH) for t in ["#","Descrição","Loja(s)","Qtd","NFs","Total"]]
        pr_data = [pr_h]
        for i, p in enumerate(prods_sorted, 1):
            desc  = (p.get("descricao") or "—")[:55]
            lojas = "/".join(sorted(p.get("lojas", set())))[:14]
            qtd   = p.get("total_qtd", 0)
            pr_data.append([
                _rl_P(str(i),           S_TD_C),
                _rl_P(desc,             S_TD),
                _rl_P(lojas,            S_TD_C),
                _rl_P(f"{qtd:g}",       S_TD_C),
                _rl_P(f"{p.get('ocorrencias',0)}x", S_TD_C),
                _rl_P(_brl_pdf(p.get("total_val",0)), S_TD_R),
            ])
        story.append(_rl_Table(pr_data, repeatRows=1,
                               colWidths=[_usable*r for r in [0.06,0.38,0.12,0.10,0.10,0.24]],
                               style=_rl_TS([
                                   ("BACKGROUND",    (0,0), (-1,0), _PDF_ACCENT),
                                   ("ROWBACKGROUNDS",(0,1), (-1,-1), [_rl_colors.white, _PDF_CARD]),
                                   ("GRID",          (0,0), (-1,-1), 0.3, _PDF_LINE),
                                   ("LEFTPADDING",   (0,0), (-1,-1), 4),
                                   ("RIGHTPADDING",  (0,0), (-1,-1), 4),
                                   ("TOPPADDING",    (0,0), (-1,-1), 3),
                                   ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                                   ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
                               ])))

    _cb = _pdf_page_cb(
        f"RESUMO DE COMPRAS — {periodo_label.upper()}",
        subtitle=f"{len(scans)} NF{'s' if len(scans)!=1 else ''}  ·  {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
    )
    doc.build(story, onFirstPage=_cb, onLaterPages=_cb)
    buf.seek(0)
    return buf.read()


# ── In-memory log buffer (for /logs) ──────────
class _MemLogHandler(logging.Handler):
    def __init__(self, maxlen: int = 300) -> None:
        super().__init__()
        self.buf: deque[str] = deque(maxlen=maxlen)
    def emit(self, record: logging.LogRecord) -> None:
        self.buf.append(self.format(record))

_mem_log = _MemLogHandler()
_mem_log.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger().addHandler(_mem_log)

# ── Activity tracker {user_id_str: {name, username, role, last_seen, count}} ──
ACTIVITY: dict[str, dict] = {}

UPLOAD_LIMITS: dict[int, float] = {}
REPORT_LIMITS: dict[int, dict[str, float]] = {}

def _load_activity() -> None:
    global ACTIVITY
    if ACTIVITY_FILE.exists():
        try:
            ACTIVITY = json.loads(ACTIVITY_FILE.read_text(encoding="utf-8"))
        except Exception:
            ACTIVITY = {}

def _save_activity() -> None:
    ACTIVITY_FILE.write_text(json.dumps(ACTIVITY, ensure_ascii=False, indent=2), encoding="utf-8")


# ──────────────────────────────────────────────
# RBAC helpers
# ──────────────────────────────────────────────
def get_role(user_id: int) -> str:
    return ROLES.get(str(user_id), "user")


def has_permission(user_id: int, permission: str) -> bool:
    return permission in ROLE_PERMISSIONS.get(get_role(user_id), [])


def _load_roles() -> None:
    """Merge persisted roles from JSON into the in-memory ROLES dict."""
    if ROLES_FILE.exists():
        try:
            saved = json.loads(ROLES_FILE.read_text(encoding="utf-8"))
            ROLES.update(saved)
        except Exception:
            pass
    # Admin from .env always wins
    if ADMIN_AUTH_TOKEN:
        ROLES[ADMIN_AUTH_TOKEN] = "admin"


def _save_roles() -> None:
    ROLES_FILE.write_text(json.dumps(ROLES, ensure_ascii=False, indent=2), encoding="utf-8")


# ──────────────────────────────────────────────
# QR helpers
# ──────────────────────────────────────────────

# WeChat QR detector singleton (CNN-based, handles tilt/distortion)
try:
    _WECHAT_QR = cv2.wechat_qrcode.WeChatQRCode()
except Exception:
    _WECHAT_QR = None


def _zbar_decode(mat: np.ndarray) -> tuple[str | None, tuple | None]:
    """Run pyzbar on a numpy array. Returns (data, bbox) or (None, None)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        results = _pyzbar.decode(mat, symbols=[_pyzbar.ZBarSymbol.QRCODE])
    for r in results:
        data = r.data.decode("utf-8", errors="replace").strip()
        if data:
            xs   = [p.x for p in r.polygon]
            ys   = [p.y for p in r.polygon]
            return data, (min(xs), min(ys), max(xs), max(ys))
    return None, None


def _cv_detect(mat: np.ndarray) -> tuple[str | None, tuple | None]:
    """Run cv2.QRCodeDetector. Returns (data, bbox) or (None, None)."""
    detector = cv2.QRCodeDetector()
    data, points, _ = detector.detectAndDecode(mat)
    if data and points is not None:
        pts    = points[0].astype(int)
        xs, ys = pts[:, 0], pts[:, 1]
        return data, (int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max()))
    return None, None


def _wechat_detect(mat: np.ndarray) -> tuple[str | None, tuple | None]:
    """Run WeChat CNN-based QR detector. Handles tilt, blur, low-contrast."""
    if _WECHAT_QR is None:
        return None, None
    try:
        texts, points = _WECHAT_QR.detectAndDecode(mat)
        for i, text in enumerate(texts):
            if text:
                pts = points[i].astype(int)
                xs, ys = pts[:, 0], pts[:, 1]
                return text, (int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max()))
    except Exception:
        pass
    return None, None


def _rotate_mat(mat: np.ndarray, angle_deg: float) -> np.ndarray:
    """Rotate image around its center."""
    h, w  = mat.shape[:2]
    cx, cy = w // 2, h // 2
    M     = cv2.getRotationMatrix2D((cx, cy), angle_deg, 1.0)
    cos   = abs(M[0, 0]); sin = abs(M[0, 1])
    nw    = int(h * sin + w * cos)
    nh    = int(h * cos + w * sin)
    M[0, 2] += (nw / 2) - cx
    M[1, 2] += (nh / 2) - cy
    return cv2.warpAffine(mat, M, (nw, nh), flags=cv2.INTER_CUBIC,
                          borderMode=cv2.BORDER_REPLICATE)


def _build_variants(img_cv: np.ndarray) -> list[tuple]:
    """
    Return list of (mat, scale) pre-processed variants to try.
    Scale is the divisor to convert detected bbox back to original coords.
    """
    h, w = img_cv.shape[:2]
    gray  = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))

    # ---------- base variants ----------
    eq     = clahe.apply(gray)
    denoise= cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
    morph  = cv2.morphologyEx(gray, cv2.MORPH_CLOSE,
                 cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)))
    sharp  = cv2.filter2D(img_cv, -1, np.array([[-1,-1,-1],[-1,9,-1],[-1,-1,-1]]))
    sharp2 = cv2.filter2D(gray,   -1, np.array([[0,-1,0],[-1,5,-1],[0,-1,0]]))
    _, otsu   = cv2.threshold(gray,  0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, otsu_i = cv2.threshold(gray,  0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    adap  = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY, 21, 5)
    adap2 = cv2.adaptiveThreshold(denoise, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                cv2.THRESH_BINARY, 31, 10)

    base = [
        (img_cv, 1), (gray, 1), (morph, 1), (eq, 1), (denoise, 1), (sharp, 1),
        (sharp2, 1), (otsu, 1), (otsu_i, 1), (adap, 1), (adap2, 1),
    ]

    # ---------- 2× upscale variants ----------
    big   = cv2.resize(img_cv, (w*2, h*2), interpolation=cv2.INTER_CUBIC)
    big_g = cv2.cvtColor(big, cv2.COLOR_BGR2GRAY)
    big_sh= cv2.filter2D(big_g, -1, np.array([[-1,-1,-1],[-1,9,-1],[-1,-1,-1]]))
    _, big_otsu = cv2.threshold(big_g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    big_adap    = cv2.adaptiveThreshold(big_g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                      cv2.THRESH_BINARY, 25, 5)
    big_eq      = clahe.apply(big_g)
    big_morph   = cv2.morphologyEx(big_g, cv2.MORPH_CLOSE,
                      cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)))

    x2 = [(big, 2), (big_g, 2), (big_sh, 2), (big_otsu, 2),
          (big_adap, 2), (big_eq, 2), (big_morph, 2)]

    # ---------- 3× upscale (for small QR codes) ----------
    big3  = cv2.resize(img_cv, (w*3, h*3), interpolation=cv2.INTER_LANCZOS4)
    big3g = cv2.cvtColor(big3, cv2.COLOR_BGR2GRAY)
    _, big3_otsu = cv2.threshold(big3g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    x3 = [(big3, 3), (big3g, 3), (big3_otsu, 3)]

    return base + x2 + x3


def _try_decoders(mat: np.ndarray) -> tuple[str | None, tuple | None]:
    """Try all decoder backends on a single mat. Returns first success."""
    for fn in (_wechat_detect, _zbar_decode, _cv_detect):
        d, b = fn(mat)
        if d:
            return d, b
    return None, None


def detect_and_crop_qr(img_bytes: bytes) -> tuple[str | None, bytes | None, tuple | None]:
    """
    Robust multi-strategy QR detector.
    Pipeline:
      1. WeChat + pyzbar + cv2 on many preprocessed variants (base, 2×, 3×)
      2. Small-angle rotations (−20° … +20° step 5°) on key variants
      3. 90° / 180° / 270° rotations (for sideways receipts)
    Returns (decoded_text, cropped_qr_png_bytes, bbox) or (None, None, None).
    """
    nparr  = np.frombuffer(img_bytes, np.uint8)
    img_cv = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img_cv is None:
        return None, None, None

    h, w = img_cv.shape[:2]

    # ── Stage 1: all preprocessed variants at original orientation ──
    variants = _build_variants(img_cv)
    data, raw_bbox, scale = None, None, 1
    for mat, sc in variants:
        data, raw_bbox = _try_decoders(mat)
        if data:
            scale = sc
            break

    # ── Stage 2: small-angle rotations on the 3 most useful variants ──
    if not data:
        gray    = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        big     = cv2.resize(img_cv, (w*2, h*2), interpolation=cv2.INTER_CUBIC)
        big_g   = cv2.cvtColor(big, cv2.COLOR_BGR2GRAY)
        rot_bases = [(img_cv, 1), (gray, 1), (big_g, 2)]
        for angle in range(-20, 25, 5):
            if angle == 0:
                continue
            for base_mat, sc in rot_bases:
                rotated = _rotate_mat(base_mat, angle)
                data, raw_bbox = _try_decoders(rotated)
                if data:
                    # bbox is in rotated space; approximate in original
                    raw_bbox = None   # can't map back easily
                    scale    = sc
                    break
            if data:
                break

    # ── Stage 3: 90° / 180° / 270° rotations ──
    if not data:
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        for code in [cv2.ROTATE_90_CLOCKWISE,
                     cv2.ROTATE_180,
                     cv2.ROTATE_90_COUNTERCLOCKWISE]:
            rot_color = cv2.rotate(img_cv, code)
            rot_gray  = cv2.rotate(gray,   code)
            for mat in (rot_color, rot_gray):
                data, raw_bbox = _try_decoders(mat)
                if data:
                    raw_bbox = None  # bbox in rotated space
                    scale    = 1
                    break
            if data:
                break

    if not data:
        return None, None, None

    # ── Crop bbox back to original image coordinates ──
    if raw_bbox is not None:
        pad   = 12
        x_min = max(raw_bbox[0] // scale - pad, 0)
        y_min = max(raw_bbox[1] // scale - pad, 0)
        x_max = min(raw_bbox[2] // scale + pad, w)
        y_max = min(raw_bbox[3] // scale + pad, h)
    else:
        # rotation-based detection: return center 60% of image as crop
        x_min = w // 5;  y_min = h // 5
        x_max = w * 4 // 5; y_max = h * 4 // 5

    cropped_cv = img_cv[y_min:y_max, x_min:x_max]
    _, png_buf = cv2.imencode(".png", cropped_cv)
    bbox       = (int(x_min), int(y_min), int(x_max), int(y_max))
    return data, png_buf.tobytes(), bbox


# NF-e UF code → State name
_UF_MAP = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP",
    "17":"TO","21":"MA","22":"PI","23":"CE","24":"RN","25":"PB",
    "26":"PE","27":"AL","28":"SE","29":"BA","31":"MG","32":"ES",
    "33":"RJ","35":"SP","41":"PR","42":"SC","43":"RS","50":"MS",
    "51":"MT","52":"GO","53":"DF",
}
_MOD_MAP = {"55": "NF-e", "65": "NFC-e"}
_AMB_MAP = {"1": "Produção", "2": "Homologação"}


def _parse_chave(chave: str) -> dict:
    """
    Decode the 44-digit NF-e/NFC-e access key.
    Layout: cUF(2) AAMM(4) CNPJ(14) mod(2) serie(3) nNF(9) tpEmis(1) cNF(8) cDV(1)
    """
    c = chave.strip()
    if len(c) != 44 or not c.isdigit():
        return {}
    aamm = c[2:6]
    try:
        ano, mes = int(aamm[:2]), int(aamm[2:])
        ano += 2000
        data_emissao = f"{ano:04d}-{mes:02d}"
    except Exception:
        data_emissao = aamm
    return {
        "uf":          _UF_MAP.get(c[0:2], c[0:2]),
        "uf_cod":      c[0:2],
        "data_emissao": data_emissao,
        "cnpj_emit":   _fmt_cnpj(c[6:20]),
        "modelo":      _MOD_MAP.get(c[20:22], c[20:22]),
        "serie":       str(int(c[22:25])),
        "numero_nf":   str(int(c[25:34])),
        "tp_emis":     c[34],
        "cod_num":     c[35:43],
        "dv":          c[43],
    }


def _fmt_cnpj(raw: str) -> str:
    c = raw.strip()
    if len(c) == 14:
        return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"
    return c


def _fmt_cpf(raw: str) -> str:
    c = raw.strip()
    if len(c) == 11:
        return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"
    return c


def _fmt_value(raw: str) -> str:
    try:
        return f"R$ {float(raw):,.2f}".replace(",","X").replace(".",",").replace("X",".")
    except Exception:
        return raw


def _as_float(s: str) -> float | None:
    """Convert Brazilian decimal string like '10,49' → 10.49, or return None."""
    try:
        return float(s.replace(".", "").replace(",", "."))
    except Exception:
        return None


# ──────────────────────────────────────────────
# SEFAZ Scraper (random fingerprint)
# ──────────────────────────────────────────────
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
]
_LANG_POOL = [
    "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "pt-BR,pt;q=0.9,en;q=0.8",
    "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
]


def _rand_headers() -> dict:
    ua   = random.choice(_UA_POOL)
    lang = random.choice(_LANG_POOL)
    mobile = "Mobile" in ua or "iPhone" in ua or "Android" in ua
    return {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           lang,
        "Accept-Encoding":           "gzip, deflate, br",
        "Cache-Control":             "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Sec-Ch-Ua-Mobile":          "?1" if mobile else "?0",
        "Connection":                "keep-alive",
    }


def scrape_sefaz(url: str, timeout: int = 15) -> dict:
    """
    Fetch the SEFAZ NFC-e public query page and extract all structured data.
    Returns dict with raw_text, parsed fields, and products list.
    """
    result: dict = {"ok": False, "raw_text": "", "error": None, "produtos": []}
    try:
        resp = requests.get(url, headers=_rand_headers(), timeout=timeout, verify=True)
        resp.encoding = resp.apparent_encoding or "utf-8"
        html = resp.text
    except Exception as exc:
        result["error"] = str(exc)
        return result

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()

    # Raw text for storage & fallback regex
    raw_text = soup.get_text(separator="\n")
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    raw_text = "\n".join(lines)
    result["raw_text"] = raw_text
    result["ok"]       = True

    parsed = _parse_sefaz_html(soup, raw_text)
    result.update(parsed)
    return result


def _parse_sefaz_html(soup: "BeautifulSoup", text: str) -> dict:
    """
    HTML-aware parser for the SEFAZ NFC-e page.
    Uses BeautifulSoup tree for structural data + regex fallbacks.
    """
    out: dict = {
        "emitente_nome":     None,
        "emitente_endereco": None,
        "cnpj_emit_page":    None,
        "numero_nf_page":    None,
        "serie_page":        None,
        "data_emissao_page": None,
        "numero_protocolo":  None,
        "data_protocolo":    None,
        "valor_total":       None,
        "valor_desconto":    None,
        "valor_pagar":       None,
        "tributos_total":    None,
        "forma_pagamento":   None,
        "consumidor":        None,
        "versao_xml":        None,
        "versao_xslt":       None,
        "qtd_itens":         None,
        "data_hora_page":    None,
        "produtos":          [],
    }

    def _fl(s: str) -> float | None:
        return _as_float(s) if s else None

    # ── 1. Emitente (name + CNPJ + address) ────────────────────────────────
    # Strategy A: soup-based — find element containing CNPJ, look at siblings/parent
    cnpj_pat = re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}")
    _emit_found = False
    for el in soup.find_all(string=cnpj_pat):
        cnpj_m = cnpj_pat.search(el)
        if not cnpj_m:
            continue
        raw_cnpj = cnpj_m.group(0)
        out["cnpj_emit_page"] = raw_cnpj
        # The element text often is "CNPJ: XX.XXX.XXX/XXXX-XX" — strip CNPJ part to get address
        parent_el = el.find_parent()
        if parent_el is None:
            break
        # Collect all text nodes in the grandparent block
        gp = parent_el.find_parent()
        block_texts = []
        if gp:
            for child in gp.children:
                t = child.get_text(separator=" ", strip=True) if hasattr(child, "get_text") else str(child).strip()
                if t:
                    block_texts.append(t)
        # First non-empty text before CNPJ line is usually the store name
        # Try to walk siblings before the CNPJ element
        prev_texts = []
        for sib in parent_el.previous_siblings:
            st = sib.get_text(separator=" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
            if st:
                prev_texts.insert(0, st)
        if prev_texts:
            # Last item before CNPJ is most likely store name
            candidate = prev_texts[-1].strip()
            if len(candidate) >= 3 and not candidate.upper().startswith("CNPJ"):
                out["emitente_nome"] = candidate
        # Address: siblings after CNPJ element
        addr_parts = []
        for sib in parent_el.next_siblings:
            st = sib.get_text(separator=" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
            if st and not re.search(r"N[uú]mero|S[eé]rie|C[oó]digo|Emiss[aã]o", st, re.IGNORECASE):
                addr_parts.append(st)
                if len(addr_parts) >= 2:
                    break
        if addr_parts:
            out["emitente_endereco"] = _clean_address(" ".join(addr_parts))
        _emit_found = out["emitente_nome"] is not None
        break

    # Strategy B: same-line regex (works when soup gives one-liner)
    if not _emit_found:
        m = re.search(
            r"^([^\n]{3,80}?)[ \t]+CNPJ[:\s]*([\d]{2}\.[\d]{3}\.[\d]{3}/[\d]{4}-[\d]{2})[ \t]*(.*?)$",
            text, re.IGNORECASE | re.MULTILINE,
        )
        if m:
            cand = m.group(1).strip()
            # Reject if clearly a page title / navigation text
            if not re.search(r"Consulta|Secretar|Fazenda|SEFAZ|publica", cand, re.IGNORECASE):
                out["emitente_nome"]     = cand
                out["cnpj_emit_page"]   = m.group(2).strip()
                out["emitente_endereco"] = _clean_address(m.group(3).strip()) or None
                _emit_found = True

    # Strategy C: two-line regex (name on line N, CNPJ on line N+1 or N+2)
    if not _emit_found:
        m = re.search(
            r"^([A-Z\u00C0-\u00DE][^\n]{2,80})\n+(?:[^\n]{0,40}\n+)?CNPJ[:\s]*([\d]{2}\.[\d]{3}\.[\d]{3}/[\d]{4}-[\d]{2})",
            text, re.IGNORECASE | re.MULTILINE,
        )
        if m:
            cand = m.group(1).strip()
            if not re.search(r"Consulta|Secretar|Fazenda|SEFAZ", cand, re.IGNORECASE):
                out["emitente_nome"]  = cand
                out["cnpj_emit_page"] = m.group(2).strip()

    # Final fallback: CNPJ alone
    if out["cnpj_emit_page"] is None:
        mc = re.search(r"CNPJ[:\s]*([\d]{2}\.[\d]{3}\.[\d]{3}/[\d]{4}-[\d]{2})", text, re.IGNORECASE)
        if mc:
            out["cnpj_emit_page"] = mc.group(1).strip()

    # ── 2. Products — parse HTML table rows ───────────────────────────────
    # Each product row has: "DESCRICAO (Código: NNNNN) Qtde.:N UN: X Vl. Unit.: N,NN"
    # followed by "Vl. Total" in next cell
    prod_pattern = re.compile(r"(.+?)\s*\(C[oó]digo:\s*(\d+)\s*\)", re.IGNORECASE)
    qty_pattern  = re.compile(
        r"Qtde\.\s*:?\s*([\d,\.]+)\s+UN[:\s]+(\S+)\s+Vl\.?\s*Unit\.?[:\s]+([\d,\.\s]+)",
        re.IGNORECASE,
    )
    total_pattern = re.compile(r"Vl\.?\s*Total\s*([\d,\.]+)", re.IGNORECASE)
    produtos = []

    for tr in soup.find_all("tr"):
        cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(["td", "th"])]
        row_text = " ".join(cells)
        pm = prod_pattern.search(row_text)
        if not pm:
            continue
        desc   = pm.group(1).strip()
        codigo = pm.group(2).strip()
        qm     = qty_pattern.search(row_text)
        tm     = total_pattern.search(row_text)
        qtd    = _fl(qm.group(1))  if qm else None
        un     = qm.group(2)       if qm else None
        vunit  = _fl(qm.group(3).strip()) if qm else None
        # vtotal: last cell or Vl. Total match
        vtotal = _fl(cells[-1]) if cells else None
        if tm:
            vtotal = _fl(tm.group(1))
        produtos.append({
            "descricao":      desc,
            "codigo":         codigo,
            "quantidade":     qtd,
            "unidade":        un,
            "valor_unitario": vunit,
            "valor_total":    vtotal,
        })

    # Fallback to raw text if table parse found nothing
    if not produtos:
        for line in text.splitlines():
            pm = prod_pattern.search(line)
            if not pm:
                continue
            desc   = pm.group(1).strip()
            codigo = pm.group(2).strip()
            qm     = qty_pattern.search(line)
            qtd    = _fl(qm.group(1))  if qm else None
            un     = qm.group(2)       if qm else None
            vunit  = _fl(qm.group(3).strip()) if qm else None
            produtos.append({
                "descricao": desc, "codigo": codigo,
                "quantidade": qtd, "unidade": un,
                "valor_unitario": vunit, "valor_total": None,
            })
    out["produtos"] = produtos

    # ── 3. Totals (apply re.DOTALL so they survive newlines) ──────────────
    def _rv(pat: str) -> float | None:
        m2 = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        return _fl(m2.group(1)) if m2 else None

    # Soup-based totals (more reliable than regex)
    def _soup_money(label_pat: str) -> float | None:
        """Find a label in soup by regex, return the next decimal-looking sibling text."""
        el = soup.find(string=re.compile(label_pat, re.IGNORECASE))
        if el is None:
            return None
        parent_el = el.find_parent()
        if parent_el is None:
            return None
        # Check current element first
        full = parent_el.get_text(separator=" ", strip=True)
        vm = re.search(r"([\d]+[,\.][\d]{2})", full)
        if vm:
            return _fl(vm.group(1))
        # Check next siblings
        for sib in parent_el.next_siblings:
            st = sib.get_text(strip=True) if hasattr(sib, "get_text") else str(sib).strip()
            if st:
                vm = re.search(r"([\d]+[,\.][\d]{2})", st)
                if vm:
                    return _fl(vm.group(1))
                break
        return None

    out["valor_total"]    = (
        _soup_money(r"Valor\s+[Tt]otal\s+R\$") or
        _rv(r"Valor\s+[Tt]otal\s+R\$\s*:?\s*([\d,\.]+)") or
        _rv(r"Valor\s+[Tt]otal[:\s]+([\d,\.]+)")
    )
    out["valor_desconto"] = (
        _soup_money(r"Desconto") or
        _rv(r"Descontos?\s+R\$\s*:?\s*([\d,\.]+)")
    )
    out["valor_pagar"]    = (
        _soup_money(r"Valor\s+a\s+pagar") or
        _rv(r"Valor\s+a\s+pagar\s+R\$\s*:?\s*([\d,\.]+)")
    )
    out["tributos_total"] = _rv(
        r"Informa[cç][aã]o\s+dos\s+Tributos\s+Totais[^\n]*Lei[^\n]*\)\s*R\$\s*([\d,\.]+)"
    )
    if out["tributos_total"] is None:
        out["tributos_total"] = _rv(r"Tributos\s+Totais[^\n]*R\$\s*([\d,\.]+)")

    # Qtd total de itens
    m2 = re.search(r"Qtd\.?\s+total\s+de\s+itens[:\s]*(\d+)", text, re.IGNORECASE)
    if m2:
        out["qtd_itens"] = int(m2.group(1))

    # ── 4. Número / Série / Emissão ───────────────────────────────────────
    m2 = re.search(
        r"N[uú]mero:\s*(\d+)\s+S[eé]rie:\s*(\d+)\s+Emiss[aã]o:\s*([\d\/]+ [\d:]+)",
        text, re.IGNORECASE,
    )
    if m2:
        out["numero_nf_page"]    = m2.group(1).strip()
        out["serie_page"]        = m2.group(2).strip()
        out["data_emissao_page"] = m2.group(3).strip()

    # ── 5. Protocolo de Autorização ───────────────────────────────────────
    m2 = re.search(
        r"Protocolo\s+de\s+Autoriza[cç][aã]o:\s*(\d+)\s+([\d\/]+ [\d:]+)",
        text, re.IGNORECASE,
    )
    if m2:
        out["numero_protocolo"] = m2.group(1).strip()
        out["data_protocolo"]   = m2.group(2).strip()

    # ── 6. Versão XML / XSLT ─────────────────────────────────────────────
    m2 = re.search(r"Vers[aã]o\s+XML:\s*([\d\.]+)", text, re.IGNORECASE)
    if m2:
        out["versao_xml"] = m2.group(1)
    m2 = re.search(r"Vers[aã]o\s+XSLT:\s*([\d\.]+)", text, re.IGNORECASE)
    if m2:
        out["versao_xslt"] = m2.group(1)

    # ── 7. Forma de Pagamento (HTML table cells give clean data) ─────────
    # Strategy: find the payment section in the soup
    pag_methods = []
    pag_section = soup.find(string=re.compile(r"Forma\s+de\s+pagamento", re.IGNORECASE))
    if pag_section:
        # Walk up to surrounding table/div then get all cells
        parent = pag_section.find_parent()
        for _ in range(4):
            if parent is None:
                break
            tbl = parent.find_parent(["table", "div", "section"])
            if tbl:
                parent = tbl
                break
            parent = parent.find_parent()
        if parent:
            # Collect text pairs: method name + value
            cells_pag = [c.get_text(strip=True) for c in parent.find_all(["td", "span", "div"]) if c.get_text(strip=True)]
            # Look for patterns like "Cartão de Débito" followed by a decimal
            i_p = 0
            while i_p < len(cells_pag):
                cp = cells_pag[i_p]
                if re.search(r"(Cart[aã]o|Dinheiro|Pix|Crédito|Débito|Cheque|Vale)", cp, re.IGNORECASE):
                    val_next = cells_pag[i_p + 1] if i_p + 1 < len(cells_pag) else ""
                    amount = re.search(r"[\d,\.]{4,}", val_next)
                    if amount:
                        pag_methods.append(f"{cp}: {val_next}")
                    else:
                        # value embedded in same cell
                        combined = re.search(
                            r"((?:Cart[aã]o|Dinheiro|Pix|Cr[eé]dito|D[eé]bito|Cheque|Vale)[^\d]*)([\d,\.]+)",
                            cp, re.IGNORECASE,
                        )
                        if combined:
                            pag_methods.append(f"{combined.group(1).strip()}: {combined.group(2)}")
                i_p += 1

    # Fallback: raw text payment block
    if not pag_methods:
        seg = re.search(
            r"Forma\s+de\s+pagamento.*?(?=Informa[cç][aã]o|Chave|Consumidor|$)",
            text, re.IGNORECASE | re.DOTALL,
        )
        if seg:
            block = seg.group(0)
            for pm_m in re.finditer(
                r"(Cart[aã]o\s+de\s+(?:D[eé]bito|Cr[eé]dito)|Dinheiro|Pix|Vale\s+\w+)[^\d]*([\d,\.]+)",
                block, re.IGNORECASE,
            ):
                pag_methods.append(f"{pm_m.group(1).strip()}: {pm_m.group(2)}")

    if pag_methods:
        out["forma_pagamento"] = " | ".join(pag_methods)
    else:
        # Last fallback: capture "Valor pago R$: X,XX" as the payment amount
        vp_m = re.search(r"Valor\s+pago\s+R\$[:\s]*([\d,\.]+)", text, re.IGNORECASE)
        if vp_m:
            out["forma_pagamento"] = f"R$ {vp_m.group(1)}"

    # ── 8. Consumidor ─────────────────────────────────────────────────────
    # After "Consumidor" header, may be "Consumidor não identificado" or CPF info
    cons_m = re.search(
        r"Consumidor\s*\n+(.*?)(?:\nData/Hora|$)",
        text, re.IGNORECASE | re.DOTALL,
    )
    if cons_m:
        cons_block = cons_m.group(1).strip().split("\n")[0].strip()
        if cons_block and cons_block not in {"CPF:", "CNPJ:"}:
            out["consumidor"] = cons_block
        else:
            full = cons_m.group(1).strip().splitlines()
            cpf_line = next((l.strip() for l in full if re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", l)), None)
            nome_line = next((l.strip() for l in full if l.strip().lower().startswith("nome")), None)
            parts = []
            if cpf_line:
                parts.append(cpf_line)
            if nome_line:
                parts.append(nome_line)
            if parts:
                out["consumidor"] = " | ".join(parts)
            elif len(full) > 1:
                out["consumidor"] = full[1].strip() or None
            else:
                out["consumidor"] = "CPF identificado" if cons_block == "CPF:" else cons_block

    # ── 9. Data/Hora da página (rodapé da NFC-e) ─────────────────────────
    dh_m = re.search(r"Data/Hora[:\s]*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", text, re.IGNORECASE)
    if dh_m:
        out["data_hora_page"] = dh_m.group(1).strip()

    return out


def parse_nfe_url(url: str) -> dict:
    """
    Parse NFC-e / NF-e QR Code URL.

    Supports both formats:
      A) Single pipe-encoded 'p' param:
         ?p=chNFe|nVersao|tpAmb[|cDest][|dhEmi][|vNF][|vICMS][|digVal][|cIdToken][|cHash]
      B) Individual query params (older format):
         ?chNFe=...&nVersao=...&tpAmb=...&vNF=...&vICMS=...&dhEmi=...etc.
    """
    parsed   = urllib.parse.urlparse(url)
    qs       = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    # ── Format A: pipe-separated p ────────────────────────────────────────
    p_raw = (qs.get("p") or [""])[0]
    if "|" in p_raw:
        parts = p_raw.split("|")
        #    0=chNFe  1=nVersao  2=tpAmb  3=cDest  4=dhEmi  5=vNF  6=vICMS
        #    7=digVal 8=cIdToken 9=cHashQRCode
        def _g(i, default="—"):
            return parts[i].strip() if i < len(parts) and parts[i].strip() else default

        chave    = _g(0)
        versao   = _g(1)
        tpAmb    = _g(2)
        cDest    = _g(3)
        dhEmi    = _g(4)
        vNF      = _g(5)
        vICMS    = _g(6)
        digVal   = _g(7)
        cIdToken = _g(8)
        cHash    = _g(9)
    # ── Format B: individual params ───────────────────────────────────────
    else:
        def _q(key, default="—"):
            v = (qs.get(key) or [""])[0].strip()
            return v if v else default
        chave    = _q("chNFe")
        versao   = _q("nVersao", _q("versao"))
        tpAmb    = _q("tpAmb")
        cDest    = _q("cDest")
        dhEmi    = _q("dhEmi")
        vNF      = _q("vNF")
        vICMS    = _q("vICMS")
        digVal   = _q("digVal")
        cIdToken = _q("cIdToken")
        cHash    = "—"

    # ── Enrich from the 44-digit access key ───────────────────────────────
    chave_info = _parse_chave(chave)

    # ── Format values ─────────────────────────────────────────────────────
    def _fmt_date(raw: str) -> str:
        try:
            if "T" in raw:
                return raw[:19].replace("T", " ")
            if len(raw) == 14:
                return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]} {raw[8:10]}:{raw[10:12]}:{raw[12:]}"
        except Exception:
            pass
        return raw

    # If dhEmi not in URL, use AAMM from key
    if dhEmi == "—" and chave_info.get("data_emissao"):
        dhEmi = chave_info["data_emissao"]

    # cDest may be CPF (11) or CNPJ (14)
    if cDest != "—":
        cDest = _fmt_cpf(cDest) if len(cDest.strip()) == 11 else _fmt_cnpj(cDest)

    return {
        "url_consulta": base_url,
        "url_completa": url,
        "chave_acesso": chave,
        "versao":       versao,
        "ambiente":     _AMB_MAP.get(tpAmb, tpAmb),
        "uf":           chave_info.get("uf", "—"),
        "modelo":       chave_info.get("modelo", "—"),
        "serie":        chave_info.get("serie", "—"),
        "numero_nf":    chave_info.get("numero_nf", "—"),
        "cnpj_emit":    chave_info.get("cnpj_emit", "—"),
        "data_emissao": _fmt_date(dhEmi),
        "cpf_cnpj_dest":cDest,
        "valor_total":  _fmt_value(vNF),
        "valor_icms":   _fmt_value(vICMS),
        "digest_value": digVal,
        "id_token":     cIdToken,
        "hash_qr":      cHash,
    }


def _h(v, default: str = "—") -> str:
    """HTML-escape a value for Telegram HTML parse_mode."""
    if v is None or v == "":
        return default
    if isinstance(v, float):
        brl = f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return brl
    s = str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_nf_form(qr_data: str, bbox: tuple, scrape: dict | None = None) -> str:
    """Return the full NF form as an HTML-formatted string (use parse_mode='HTML')."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    is_url    = qr_data.startswith("http")
    nf        = parse_nfe_url(qr_data) if is_url else {}
    sc        = scrape or {}

    def _brl(v, default="—"):
        if v is None or v == "":
            return default
        try:
            return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (TypeError, ValueError):
            return _h(v, default)

    if is_url and nf:
        ch = nf["chave_acesso"].replace(" ", "")
        ch_fmt = " ".join(ch[i:i+4] for i in range(0, len(ch), 4)) if ch.isdigit() else ch

        nome     = _h(sc.get("emitente_nome") or nf.get("cnpj_emit"))
        addr_raw  = sc.get("emitente_endereco") or ""
        endereco  = _h(_clean_address(addr_raw) or None)
        cnpj     = _h(sc.get("cnpj_emit_page") or nf.get("cnpj_emit"))
        numero   = _h(sc.get("numero_nf_page") or nf.get("numero_nf"))
        serie    = _h(sc.get("serie_page")     or nf.get("serie"))
        emissao  = _h(sc.get("data_emissao_page") or nf.get("data_emissao"))
        loja_id  = sc.get("loja_id", "")

        lines = [
            "╔══════════════════════════════════════╗",
            "║      🧾  NOTA FISCAL ELETRÔNICA      ║",
            "╚══════════════════════════════════════╝",
            "",
            f"🏷️  Modelo          : {_h(nf.get('modelo'))}",
            f"🏙️  Estado (UF)     : {_h(nf.get('uf'))}",
            f"🎭  Ambiente        : {_h(nf.get('ambiente'))}",
            f"📋  Versão NF-e     : {_h(nf.get('versao'))}",
            "",
            "────────────────────────────────────",
            "🏢  Emitente",
            "────────────────────────────────────",
            f"🏪  Nome            : {nome}" + (f"  [{loja_id}]" if loja_id else ""),
            f"🆔  CNPJ            : {cnpj}",
        ]
        if endereco != "—":
            lines.append(f"📍  Endereço        : {endereco}")
        lines += [
            f"📂  Série           : {serie}",
            f"🔢  Número NF       : {numero}",
            f"📅  Emissão         : {emissao}",
        ]

        if nf.get("cpf_cnpj_dest", "—") != "—":
            lines.append(f"👤  CPF/CNPJ Dest   : {_h(nf['cpf_cnpj_dest'])}")

        # Products
        produtos = sc.get("produtos", [])
        if produtos:
            lines += [
                "",
                "────────────────────────────────────",
                f"🛒  Produtos ({len(produtos)} item(s))",
                "────────────────────────────────────",
            ]
            for p in produtos:
                qtd   = f"{p['quantidade']:g}" if p.get("quantidade") is not None else "?"
                un    = _h(p.get("unidade") or "")
                vunit = _brl(p.get("valor_unitario"))
                vtot  = _brl(p.get("valor_total"))
                lines.append(f"  📦 {_h(p.get('descricao', '?'))}")
                lines.append(f"     Cód: {_h(p.get('codigo', '?'))} | Qtd: {qtd} {un}")
                lines.append(f"     Unit: {vunit}  Total: {vtot}")

        # Values
        vt    = sc.get("valor_total")
        vd    = sc.get("valor_desconto")
        vp    = sc.get("valor_pagar")
        vicms = _h(nf.get("valor_icms", "—"))
        trib  = sc.get("tributos_total")

        lines += [
            "",
            "────────────────────────────────────",
            "💰  Valores",
            "────────────────────────────────────",
            f"💵  Total NF        : {_brl(vt) if vt is not None else _h(nf.get('valor_total'))}",
            f"🏷️  Descontos       : {_brl(vd)}",
            f"💳  Valor a pagar   : {_brl(vp)}",
            f"📊  Valor ICMS      : {vicms}",
            f"🏛️  Tributos (12741): {_brl(trib)}",
        ]

        pag = sc.get("forma_pagamento")
        if pag:
            lines.append(f"💰  Pagamento       : {_h(pag)}")

        cons = sc.get("consumidor")
        if cons:
            lines += ["", f"👤  Consumidor      : {_h(cons)}"]

        proto       = _h(sc.get("numero_protocolo"))
        dt_proto    = _h(sc.get("data_protocolo"))
        versao_xml  = _h(sc.get("versao_xml"))
        versao_xslt = _h(sc.get("versao_xslt"))

        lines += [
            "",
            "────────────────────────────────────",
            "📜  Autorização",
            "────────────────────────────────────",
            f"🔏  Protocolo       : {proto}",
            f"📅  Data protocolo  : {dt_proto}",
            f"📄  Versão XML      : {versao_xml}",
            f"📑  Versão XSLT     : {versao_xslt}",
        ]

        data_hora = _h(sc.get("data_hora_page"))
        if data_hora != "—":
            lines.append(f"🕐  Data/Hora NFC-e  : {data_hora}")

        lines += [
            "",
            "────────────────────────────────────",
            "🔑  Chave de Acesso",
            "────────────────────────────────────",
            f"<code>{ch_fmt}</code>",
            "",
            f"🌐  URL QR Code     : {_h(qr_data)}",
            f"🕒  Lido em         : {timestamp}",
        ]
    else:
        lines = [
            "╔══════════════════════════════════════╗",
            "║          🧾  QR CODE                 ║",
            "╚══════════════════════════════════════╝",
            "",
            "📄  Conteúdo:",
            f"<code>{_h(qr_data)}</code>",
            "",
            f"🕒  Lido em: {timestamp}",
        ]

    return "\n".join(lines)


# ──────────────────────────────────────────────
# Activity tracker middleware
# ──────────────────────────────────────────────
async def track_activity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Passive middleware: records every user who sends a message."""
    user = update.effective_user
    if not user or user.is_bot:
        return
    uid = str(user.id)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    entry = ACTIVITY.setdefault(uid, {
        "name": "", "username": "", "role": "", "first_seen": now,
        "last_seen": now, "message_count": 0,
    })
    entry["name"]          = f"{user.first_name or ''} {user.last_name or ''}".strip() or "—"
    entry["username"]      = f"@{user.username}" if user.username else "—"
    entry["role"]          = get_role(user.id)
    entry["last_seen"]     = now
    entry["message_count"] = entry.get("message_count", 0) + 1
    _save_activity()


# ──────────────────────────────────────────────
# /addrole  –  add_user_role  (admin)
# ──────────────────────────────────────────────
async def addrole_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "add_user_role"):
        await message.reply_text("🚫 Apenas admins podem atribuir roles."); return

    args = context.args
    valid_roles = list(ROLE_PERMISSIONS.keys())
    if len(args) != 2 or args[1] not in valid_roles:
        await message.reply_text(
            f"\U0001f4cc Uso: `/addrole <user_id> <role>`\n"
            f"Roles disponíveis: {', '.join(f'`{r}`' for r in valid_roles)}",
            parse_mode="Markdown",
        )
        return

    target_id, role = args[0], args[1]
    if not target_id.lstrip("-").isdigit():
        await message.reply_text("❌ user_id inválido. Use o ID numérico.", parse_mode="Markdown"); return

    ROLES[target_id] = role
    _save_roles()
    # Update activity record if user is known
    if target_id in ACTIVITY:
        ACTIVITY[target_id]["role"] = role
        _save_activity()

    logger.info("Role '%s' atribuído ao user %s por %s", role, target_id, user.id)
    await message.reply_text(
        f"✅ Role `{role}` atribuído ao usuário `{target_id}` com sucesso.",
        parse_mode="Markdown",
    )


# ──────────────────────────────────────────────
# /removerole  –  remove_user_role  (admin)
# ──────────────────────────────────────────────
async def removerole_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "remove_user_role"):
        await message.reply_text("🚫 Apenas admins podem remover roles."); return

    args = context.args
    if len(args) != 1:
        await message.reply_text("📌 Uso: `/removerole <user_id>`", parse_mode="Markdown"); return

    target_id = args[0]
    if not target_id.lstrip("-").isdigit():
        await message.reply_text("❌ user_id inválido.", parse_mode="Markdown"); return

    # Never strip the .env admin
    if target_id == ADMIN_AUTH_TOKEN:
        await message.reply_text("⚠️ Não é possível remover o admin principal definido no .env."); return

    removed = ROLES.pop(target_id, None)
    _save_roles()
    if target_id in ACTIVITY:
        ACTIVITY[target_id]["role"] = "user"
        _save_activity()

    if removed:
        logger.info("Role '%s' removido do user %s por %s", removed, target_id, user.id)
        await message.reply_text(
            f"✅ Role `{removed}` removido. Usuário `{target_id}` voltou para `user`.",
            parse_mode="Markdown",
        )
    else:
        await message.reply_text(f"ℹ️ Usuário `{target_id}` já era `user` (sem role atribuído).",
                                 parse_mode="Markdown")


# ──────────────────────────────────────────────
# /logs  –  view_logs  (admin)
# ──────────────────────────────────────────────
async def logs_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "view_logs"):
        await message.reply_text("🚫 Apenas admins podem ver logs."); return

    n = 20
    if context.args and context.args[0].isdigit():
        n = min(int(context.args[0]), 100)

    recent = list(_mem_log.buf)[-n:]
    if not recent:
        await message.reply_text("📭 Nenhum log registrado ainda."); return

    text = "\n".join(recent)
    # Telegram max caption/message = 4096 chars
    if len(text) > 3900:
        text = "…(truncado)\n" + text[-3900:]

    await message.reply_text(
        f"```\n{text}\n```",
        parse_mode="Markdown",
    )


# ──────────────────────────────────────────────
# /resume  –  request_resume  (admin + mod)
# ──────────────────────────────────────────────
async def resume_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "request_resume"):
        await message.reply_text("🚫 Sem permissão para /resume."); return

    PERIODOS = {"dia": 1, "semana": 7, "quinzena": 15, "mes": 30}

    # ── No args → show usage + bot stats ─────────────────────────────────
    if not context.args:
        total_users = len(ACTIVITY)
        role_counts: dict[str, int] = {}
        total_msgs = 0
        for entry in ACTIVITY.values():
            r = entry.get("role", "user")
            role_counts[r] = role_counts.get(r, 0) + 1
            total_msgs += entry.get("message_count", 0)

        nf_files = list((OUTPUT_DIR / "json").glob("nf_*.json"))
        nf_count = len(nf_files)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        role_lines = "\n".join(f"  {r}: {c}" for r, c in sorted(role_counts.items()))

        text = (
            "╔═══════════════════════════════╗\n"
            "║      📊  RESUMO DO BOT        ║\n"
            "╚═══════════════════════════════╝\n"
            "\n"
            f"👥  Usuários únicos   : {total_users}\n"
            f"💬  Mensagens receb.  : {total_msgs}\n"
            f"🧾  NFs processadas   : {nf_count}\n"
            "\n"
            "🎭  Distribuição de roles:\n"
            f"{role_lines if role_lines else '  (sem dados)'}\n"
            "\n"
            "────────────────────────────────────\n"
            "📅  Relatórios por período disponíveis:\n"
            "  /resume dia       — últimas 24 h\n"
            "  /resume semana    — últimos 7 dias\n"
            "  /resume quinzena  — últimos 15 dias\n"
            "  /resume mes       — últimos 30 dias\n"
            "\n"
            f"🕒  Em: {timestamp}"
        )
        await message.reply_text(text)
        return

    # ── Period arg → purchase summary ────────────────────────────────────
    raw_period = context.args[0].lower()
    days = PERIODOS.get(raw_period)
    if days is None:
        await message.reply_text(
            "❓ Período inválido. Use: dia | semana | quinzena | mes"
        )
        return

    now = datetime.now(timezone.utc).timestamp()
    user_reports = REPORT_LIMITS.setdefault(user.id, {})
    last_report = user_reports.get(raw_period, 0)
    if now - last_report < 3600:
        await message.reply_text(f"⏳ Aguarde 1 hora entre solicitações de relatórios ({raw_period}).")
        return
    user_reports[raw_period] = now

    periodo_label = {1: "Diário (1 dia)", 7: "Semanal (7 dias)",
                     15: "Quinzenal (15 dias)", 30: "Mensal (30 dias)"}[days]
    cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    try:
        all_scans = _load_csv_scans()
        scans = [s for s in all_scans if s.get("scan_at", "") >= cutoff_str]

        def _flt(v):
            try: return float(v)
            except (TypeError, ValueError): return 0.0

        total_scans = len(scans)
        total_users_period = len({s.get("user_id") for s in scans})
        vt_sum = sum(_flt(s.get("valor_total")) or _flt(s.get("valor_pagar")) for s in scans)
        vd_sum = sum(_flt(s.get("valor_desconto")) for s in scans)
        vp_sum = sum(_flt(s.get("valor_pagar")) for s in scans)

        period_ids = {s["scan_id"] for s in scans}
        all_prods  = _load_csv_produtos()
        period_prods = [p for p in all_prods if p.get("scan_id") in period_ids]

        from collections import defaultdict

        # Products aggregated by code+name, tracking loja_ids
        prod_agg: dict = defaultdict(lambda: {"descricao": "", "total_qtd": 0.0, "total_val": 0.0, "ocorrencias": 0, "lojas": set()})
        for p in period_prods:
            key = p.get("codigo") or p.get("descricao") or "?"
            prod_agg[key]["descricao"]   = p.get("descricao") or key
            prod_agg[key]["total_qtd"]  += _flt(p.get("quantidade"))
            prod_agg[key]["total_val"]  += _flt(p.get("valor_total"))
            prod_agg[key]["ocorrencias"] += 1
            lid = p.get("loja_id") or ""
            if lid:
                prod_agg[key]["lojas"].add(lid)
        prods_sorted = sorted(prod_agg.values(), key=lambda x: x["total_val"], reverse=True)

        # Store breakdown — fallback valor_pagar when valor_total is 0
        emit_agg: dict = defaultdict(lambda: {"nome": "", "loja_id": "", "cnt": 0, "vt": 0.0})
        for s in scans:
            key = s.get("cnpj_emit") or s.get("emitente_nome") or "?"
            emit_agg[key]["nome"]    = s.get("emitente_nome") or key
            emit_agg[key]["loja_id"] = s.get("loja_id", "")
            emit_agg[key]["cnt"]    += 1
            vt = _flt(s.get("valor_total")) or _flt(s.get("valor_pagar"))
            emit_agg[key]["vt"]     += vt
        emit_sorted = sorted(emit_agg.values(), key=lambda x: x["cnt"], reverse=True)

        # Payment breakdown — parse and deduplicate by method name
        pag_agg: dict = defaultdict(lambda: {"total": 0.0, "cnt": 0})
        for s in scans:
            fp = s.get("forma_pagamento")
            if not fp:
                continue
            for part in fp.split(" | "):
                part = part.strip()
                pm2 = re.match(r"(.+?):\s*([\d,\.]+)$", part)
                if pm2:
                    raw_meth = pm2.group(1).strip()
                    amount   = _flt(pm2.group(2).replace(",", "."))
                else:
                    rv = re.match(r"R\$\s*([\d,\.]+)$", part)
                    raw_meth = "Outros"
                    amount   = _flt(rv.group(1).replace(",", ".")) if rv else 0
                # Normalize: anything without Crédito/Débito/Pix/Dinheiro → Outros
                if re.search(r"cr[eé]dito", raw_meth, re.IGNORECASE):
                    meth = "Cartão de Crédito"
                elif re.search(r"d[eé]bito", raw_meth, re.IGNORECASE):
                    meth = "Cartão de Débito"
                elif re.search(r"pix", raw_meth, re.IGNORECASE):
                    meth = "Pix"
                elif re.search(r"dinheiro|espécie|especie", raw_meth, re.IGNORECASE):
                    meth = "Dinheiro"
                elif re.search(r"vale", raw_meth, re.IGNORECASE):
                    meth = "Vale"
                else:
                    meth = "Outros"
                pag_agg[meth]["total"] += amount
                pag_agg[meth]["cnt"]   += 1
        pag_sorted = sorted(pag_agg.items(), key=lambda x: x[1]["total"], reverse=True)

    except Exception as exc:
        await message.reply_text(f"❌ Erro ao gerar relatório: {exc}"); return

    def _brl(v: float) -> str:
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    lines = [
        "╔══════════════════════════════════╗",
        f"║  📊  RESUMO — {periodo_label:^19}║",
        "╚══════════════════════════════════╝",
        "",
        f"🧾  NFs escaneadas    : {total_scans}",
        f"👥  Usuários ativos   : {total_users_period}",
        f"💵  Total gasto       : {_brl(vt_sum)}",
        f"🏷️  Total descontos   : {_brl(vd_sum)}",
        f"💳  Total pago        : {_brl(vp_sum)}",
    ]

    # Top stores
    if emit_sorted:
        lines += ["", "────────────────────────────────────",
                  "🏪  Locais de compra", "────────────────────────────────────"]
        for e in emit_sorted[:8]:
            nome = (e["nome"] or "—")[:35]
            lid_tag = f" [{e['loja_id']}]" if e.get("loja_id") else ""
            lines.append(f"  🏬 {nome}{lid_tag}")
            lines.append(f"       {e['cnt']}x — {_brl(e['vt'])}")

    # Payment methods
    if pag_sorted:
        lines += ["", "────────────────────────────────────",
                  "💳  Formas de pagamento", "────────────────────────────────────"]
        for forma, data in pag_sorted:
            lines.append(f"  • {str(forma)[:40]}: {_brl(data['total'])}  ({data['cnt']}x)")

    # All products
    if prods_sorted:
        lines += ["", "────────────────────────────────────",
                  "🛒  Produtos comprados no período", "────────────────────────────────────"]
        for i, p in enumerate(prods_sorted, 1):
            desc = (p["descricao"] or "—")[:40]
            qtd  = p["total_qtd"]
            val  = _brl(p["total_val"])
            oc   = p["ocorrencias"]
            loja_tags = "/".join(sorted(p.get("lojas", set())))
            loja_str  = f" [{loja_tags}]" if loja_tags else ""
            lines.append(f"  {i:>3}. {desc}{loja_str}")
            lines.append(f"       Qtd: {qtd:g}  Total: {val}  ({oc}x)")
    else:
        lines += ["", "  (sem produtos registrados no período)"]

    lines += ["", f"🕒  Gerado em: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"]

    text = "\n".join(lines)
    if len(text) > 3950:
        text = text[:3950] + "\n…(truncado)"
    await message.reply_text(f"📝 [TEXTO] Resumo:\n\n{text}")

    # Generate PDF and save + send
    ts_now      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    resume_base = OUTPUT_DIR / "txt" / f"resume_{user.id}_{ts_now}.txt"
    resume_base.write_text(text, encoding="utf-8")
    try:
        pdf_bytes = await asyncio.to_thread(
            _build_resume_pdf,
            scans, emit_sorted, pag_sorted, prods_sorted,
            periodo_label, vt_sum, vd_sum, vp_sum,
        )
        pdf_fname = f"Resumo_{periodo_label.replace(' ','_')}_{ts_now}.pdf"
        await message.reply_document(
            document=pdf_bytes,
            filename=pdf_fname,
            caption=f"📄 [PDF] {periodo_label} — {len(scans)} NF{'s' if len(scans)!=1 else ''}",
        )
        
        pdf_path = OUTPUT_DIR / "pdf" / pdf_fname
        pdf_path.write_bytes(pdf_bytes)
        logger.info("Resume PDF saved: %s", pdf_path)
    except Exception as exc:
        logger.warning("Resume PDF failed: %s", exc)
        # Fallback to infographic image
        try:
            img_bytes = await asyncio.to_thread(
                _render_resume_image,
                scans, emit_sorted, pag_sorted, prods_sorted,
                periodo_label, vt_sum, vd_sum, vp_sum,
            )
            await message.reply_photo(photo=img_bytes)
            resume_base.with_suffix(".png").write_bytes(img_bytes)
        except Exception as exc2:
            logger.warning("Resume image fallback also failed: %s", exc2)


# ──────────────────────────────────────────────
# /users  –  see_users_sending_messages  (admin)
# ──────────────────────────────────────────────
async def users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "see_users_sending_messages"):
        await message.reply_text("🚫 Sem permissão para /users."); return

    if not ACTIVITY:
        await message.reply_text("📭 Nenhum usuário registrado ainda."); return

    # Sort by last_seen descending
    sorted_users = sorted(ACTIVITY.items(), key=lambda kv: kv[1].get("last_seen", ""), reverse=True)

    lines = [
        "╔═══════════════════════════════╗",
        "║   👥  USUÁRIOS ATIVOS         ║",
        "╚═══════════════════════════════╝",
        "",
    ]
    for uid, data in sorted_users:
        role    = data.get("role", "user")
        name    = _h(data.get("name", "—"))
        uname   = _h(data.get("username", "—"))
        seen    = data.get("last_seen", "—")
        count   = data.get("message_count", 0)
        icon    = {"admin": "🔴", "moderator": "🟡"}.get(role, "⚪")
        lines.append(
            f"{icon} <code>{uid}</code> | {name} ({uname})\n"
            f"   Role: <code>{role}</code> | Msgs: {count} | Visto: {seen[:10]}"
        )
        lines.append("")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n…(lista truncada)"

    await message.reply_text(text, parse_mode="HTML")


# ──────────────────────────────────────────────
# /help  –  help_message  (all roles)
# ──────────────────────────────────────────────
async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user
    role    = get_role(user.id)
    perms   = ROLE_PERMISSIONS.get(role, [])

    PERM_HELP = {
        "request_myid":              "  /myid                        — seu perfil completo",
        "send_nf":                   "  /nf                          — ler QR de nota fiscal (foto + /nf)",
        "request_resume":            "  /resume                      — resumo de atividade\n"
                                     "  /relatorio [dia|semana|quinzena|mes]",
        "see_users_sending_messages":"  /users                       — lista de usuários ativos",
        "view_logs":                 "  /logs [n]                    — últimas n linhas de log",
        "add_user_role":             "  /addrole <id> <role>         — atribuir role",
        "remove_user_role":          "  /removerole <id>             — remover role",
        "help_message":              "  /help                        — esta mensagem",
    }

    cmd_lines = [PERM_HELP[p] for p in perms if p in PERM_HELP]
    roles_readable = {"admin": "🔴 Admin", "moderator": "🟡 Moderador", "user": "⚪ Usuário"}

    await message.reply_text(
        f"╔══════════════════════════════╗\n"
        f"║        🤖  AJUDA             ║\n"
        f"╚══════════════════════════════╝\n"
        f"\n"
        f"Seu nível: {roles_readable.get(role, role)}\n"
        f"\n"
        f"Comandos disponíveis:\n"
        + "\n".join(cmd_lines),
        parse_mode="Markdown",
    )


# ──────────────────────────────────────────────
# /nf  –  photo (caption /nf) OR reply to photo
# ──────────────────────────────────────────────
async def nf_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "send_nf"):
        await message.reply_text(
            f"🚫 Sem permissão para /nf.\nSeu nível: `{get_role(user.id)}`",
            parse_mode="Markdown",
        )
        return

    now = datetime.now(timezone.utc).timestamp()
    last_upload = UPLOAD_LIMITS.get(user.id, 0)
    if now - last_upload < 50:
        await message.reply_text("⏳ Aguarde 50 segundos entre uploads de notas.")
        return
    UPLOAD_LIMITS[user.id] = now

    photo_msg = message
    if not message.photo and message.reply_to_message and message.reply_to_message.photo:
        photo_msg = message.reply_to_message

    if not photo_msg.photo:
        await message.reply_text(
            "📎 Envie a foto da nota fiscal com a legenda `/nf`\n"
            "ou responda uma foto já enviada com `/nf`.",
            parse_mode="Markdown",
        )
        return

    wait_msg = await message.reply_text("🔍 Processando nota fiscal, aguarde…")

    tg_file   = await photo_msg.photo[-1].get_file()
    img_buf   = io.BytesIO()
    await tg_file.download_to_memory(img_buf)

    qr_data, cropped_png, bbox = detect_and_crop_qr(img_buf.getvalue())

    await wait_msg.delete()

    if not qr_data:
        await message.reply_text(
            "❌ Nenhum QR Code encontrado.\n"
            "Tente com melhor iluminação e ângulo reto à nota."
        )
        return

    nf_meta = parse_nfe_url(qr_data) if qr_data.startswith("http") else {}

    # ── Duplicate NF check (before scraping to avoid unnecessary requests) ─
    dup_id = _is_duplicate_nf(nf_meta.get("chave_acesso"))
    if dup_id:
        chave_show = (nf_meta.get("chave_acesso") or "")[:22]
        await message.reply_photo(photo=cropped_png)
        await message.reply_text(
            f"⚠️  Esta NF já foi registrada anteriormente.\n"
            f"Scan ID: <code>#{dup_id}</code>  —  chave: <code>{chave_show}…</code>\n"
            f"Use /resume dia para ver seus dados.",
            parse_mode="HTML",
        )
        return

    # ── SEFAZ scraping ────────────────────────────────────────────────────
    scrape: dict = {}
    if qr_data.startswith("http"):
        scrape_msg = await message.reply_text("🌐 Consultando dados na SEFAZ…")
        try:
            scrape = await asyncio.to_thread(scrape_sefaz, qr_data)
        except Exception as exc:
            logger.warning("SEFAZ scrape failed: %s", exc)
            scrape = {"ok": False, "error": str(exc)}
        finally:
            await scrape_msg.delete()

    # ── Persist to CSV ────────────────────────────────────────────────────
    scan_at  = datetime.now(timezone.utc).isoformat()
    ts_tag   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = OUTPUT_DIR / f"nf_{user.id}_{ts_tag}"
    qr_json_path = str(base_name.with_suffix(".json"))

    def _row_val(key: str, default=None):
        v = scrape.get(key)
        return v if v is not None else default

    loja_id = _get_or_create_loja(
        cnpj     = (_row_val("cnpj_emit_page") or nf_meta.get("cnpj_emit") or "").strip() or None,
        nome     = _row_val("emitente_nome"),
        endereco = _row_val("emitente_endereco"),
    )

    scan_record = {
        "scan_at":           scan_at,
        "user_id":           user.id,
        "qr_url":            qr_data if qr_data.startswith("http") else None,
        "qr_json_path":      qr_json_path,
        "chave_acesso":      nf_meta.get("chave_acesso"),
        "loja_id":           loja_id,
        "ambiente":          nf_meta.get("ambiente"),
        "uf":                nf_meta.get("uf"),
        "modelo":            nf_meta.get("modelo"),
        "serie":             _row_val("serie_page") or nf_meta.get("serie"),
        "numero_nf":         _row_val("numero_nf_page") or nf_meta.get("numero_nf"),
        "cnpj_emit":         _row_val("cnpj_emit_page") or nf_meta.get("cnpj_emit"),
        "emitente_nome":     _row_val("emitente_nome"),
        "emitente_endereco": _row_val("emitente_endereco"),
        "data_emissao":      _row_val("data_emissao_page") or nf_meta.get("data_emissao"),
        "numero_protocolo":  _row_val("numero_protocolo"),
        "data_protocolo":    _row_val("data_protocolo"),
        "valor_total":       _row_val("valor_total"),
        "valor_desconto":    _row_val("valor_desconto"),
        "valor_pagar":       _row_val("valor_pagar"),
        "valor_icms":        None,
        "tributos_total":    _row_val("tributos_total"),
        "qtd_itens":         _row_val("qtd_itens"),
        "forma_pagamento":   _row_val("forma_pagamento"),
        "consumidor":        _row_val("consumidor"),
        "versao_xml":        _row_val("versao_xml"),
        "versao_xslt":       _row_val("versao_xslt"),
        "data_hora_page":    _row_val("data_hora_page"),
        "raw_text":          scrape.get("raw_text", ""),
    }
    scan_id = None
    try:
        scan_id = _save_to_csv(scan_record, scrape.get("produtos", []))
    except Exception as exc:
        logger.error("CSV save failed: %s", exc)

    # ── Save output files (PNG + JSON) ────────────────────────────────────
    # Save QR crop only to the dedicated NF_QR_CODE folder (not duplicated in output/)
    png_fname = OUTPUT_DIR / "png" / f"qr_{scan_id or ts_tag}_{ts_tag}.png"
    qr_fname = OUTPUT_QR / f"qr_{scan_id or ts_tag}_{ts_tag}.png"
    qr_fname.write_bytes(cropped_png)
    png_fname.write_bytes(cropped_png)
    
    json_path = OUTPUT_DIR / "json" / f"nf_{scan_id or ts_tag}_{ts_tag}.json"
    json_path.write_text(
        json.dumps({
            "timestamp":      ts_tag,
            "scan_at":        scan_at,
            "scan_id":        scan_id,
            "user_id":        user.id,
            "qr_url":         qr_data,
            "qr_data":        qr_data,
            "qr_json_path":   qr_json_path,
            "bbox":           list(bbox),
            "nf_meta":        nf_meta,
            "data_hora_page": scrape.get("data_hora_page"),
            "sefaz_scraped":  {k: v for k, v in scrape.items() if k != "raw_text"},
            "qr_image_b64":   base64.b64encode(cropped_png).decode(),
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info("NF processed | user=%s | scan_id=%s | qr=%.60s", user.id, scan_id, qr_data)

    # ── Build reply ───────────────────────────────────────────────────────
    keyboard = None
    if qr_data.startswith("http"):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Consultar na SEFAZ", url=qr_data)],
        ])

    # Send cropped QR image first (no caption — full data goes below)
    await message.reply_photo(photo=cropped_png, caption="🖼️ [IMAGEM] QR Code Cortado")

    # Send full form as text (HTML mode — safe against special chars in names)
    form_text = format_nf_form(qr_data, bbox, scrape)
    if len(form_text) > 4050:
        form_text = form_text[:4044] + "\n…"
    await message.reply_text(f"📝 [TEXTO] Dados da NF:\n\n{form_text}", parse_mode="HTML", reply_markup=keyboard)

    # Generate and send PDF
    try:
        sr_pdf = dict(scan_record)
        sr_pdf["scan_id"] = scan_id
        pdf_bytes = await asyncio.to_thread(_build_nf_pdf, sr_pdf, scrape, nf_meta, cropped_png)
        fname = f"NF_{scan_id}_{ts_tag}.pdf"
        await message.reply_document(
            document=pdf_bytes, filename=fname,
            caption=f"📄 [PDF] Nota Fiscal — Scan #{scan_id}",
        )
        # Also save PDF to output folder
        (OUTPUT_DIR / "pdf" / fname).write_bytes(pdf_bytes)
    except Exception as exc:
        logger.warning("NF PDF generation failed: %s", exc)
        # Fallback to infographic image if PDF fails
        try:
            sr_img = dict(scan_record)
            sr_img["scan_id"] = scan_id
            img_bytes = await asyncio.to_thread(_render_nf_image, sr_img, scrape, nf_meta)
            await message.reply_photo(photo=img_bytes, caption="📊 [IMAGEM] Infográfico da NF")
        except Exception as exc2:
            logger.warning("NF infographic fallback also failed: %s", exc2)


async def myid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user
    chat    = update.effective_chat

    if not has_permission(user.id, "request_myid"):
        await message.reply_text("🚫 Sem permissão para /myid.")
        return

    if chat.type != "private":
        await message.reply_text("⚠️ Este comando só funciona em DM comigo.")
        return

    user_id         = user.id
    first_name      = user.first_name or "—"
    last_name       = user.last_name  or "—"
    username        = f"@{user.username}" if user.username else "—"
    language_code   = user.language_code or "—"
    is_bot          = "✅ Sim" if user.is_bot               else "❌ Não"
    is_premium      = "✅ Sim" if user.is_premium           else "❌ Não"
    added_to_attach = "✅ Sim" if user.added_to_attachment_menu else "❌ Não"
    profile_link    = f"tg://user?id={user_id}"

    photos      = await context.bot.get_user_profile_photos(user_id, limit=1)
    has_photo   = photos.total_count > 0
    photo_count = photos.total_count
    timestamp   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    form = (
        "╔══════════════════════════════╗\n"
        "║      📋  MEU  PERFIL         ║\n"
        "╚══════════════════════════════╝\n"
        "\n"
        f"🆔  ID               : <code>{user_id}</code>\n"
        f"🎭  Papel (role)     : <code>{get_role(user_id)}</code>\n"
        f"👤  Primeiro nome    : {_h(first_name)}\n"
        f"👤  Sobrenome        : {_h(last_name)}\n"
        f"🔖  Username / Nick  : {_h(username)}\n"
        f"🌐  Idioma           : {_h(language_code)}\n"
        f"🤖  É um bot?        : {is_bot}\n"
        f"⭐  Telegram Premium : {is_premium}\n"
        f"📎  No menu attach.  : {added_to_attach}\n"
        f"🖼️  Fotos de perfil  : {photo_count} foto(s)\n"
        f"📸  Tem foto atual?  : {'✅ Sim' if has_photo else '❌ Não'}\n"
        "\n"
        "ℹ️  Telefone: não acessível por bots\n"
        "    (compartilhe via botão 'Contato').\n"
        "\n"
        f"🕒  Gerado em: {timestamp}"
    )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("👤 Ver perfil", url=profile_link)]])

    if has_photo:
        file_id = photos.photos[0][-1].file_id
        await message.reply_photo(photo=file_id, caption=form,
                                  parse_mode="HTML", reply_markup=keyboard)
    else:
        await message.reply_text(text=form, parse_mode="HTML", reply_markup=keyboard)

    logger.info("Served /myid to user %s (%s)", user_id, username)


# ──────────────────────────────────────────────
# /start
# ──────────────────────────────────────────────
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    role  = get_role(update.effective_user.id)
    roles_readable = {"admin": "🔴 Admin", "moderator": "🟡 Moderador", "user": "⚪ Usuário"}
    await update.effective_message.reply_text(
        f"👋 Olá! Nível: {roles_readable.get(role, role)}\n\n"
        "Use /help para ver os comandos disponíveis.",
        parse_mode="Markdown",
    )


# ──────────────────────────────────────────────
# /relatorio  –  product summary by period (admin + mod)
# ──────────────────────────────────────────────
async def relatorio_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user

    if not has_permission(user.id, "request_resume"):
        await message.reply_text("🚫 Sem permissão para /relatorio."); return

    # Parse period arg: dia | semana | quinzena | mes  (default: semana)
    PERIODOS = {"dia": 1, "semana": 7, "quinzena": 15, "mes": 30}
    raw_period = (context.args[0].lower() if context.args else "semana")
    if raw_period not in PERIODOS: raw_period = "semana"
    
    now = datetime.now(timezone.utc).timestamp()
    user_reports = REPORT_LIMITS.setdefault(user.id, {})
    last_report = user_reports.get(raw_period, 0)
    if now - last_report < 3600:
        await message.reply_text(f"⏳ Aguarde 1 hora entre solicitações de relatórios ({raw_period}).")
        return
    user_reports[raw_period] = now

    days = PERIODOS.get(raw_period, 7)
    periodo_label = {1: "Diário (1 dia)", 7: "Semanal (7 dias)",
                     15: "Quinzenal (15 dias)", 30: "Mensal (30 dias)"}.get(days, f"{days} dias")

    cutoff = datetime.now(timezone.utc).replace(microsecond=0)
    cutoff_str = (cutoff - timedelta(days=days)).isoformat()

    try:
        all_scans = _load_csv_scans()
        scans = [s for s in all_scans if s.get("scan_at", "") >= cutoff_str]

        def _flt(v):
            try: return float(v)
            except (TypeError, ValueError): return 0.0

        total_scans = len(scans)
        total_users = len({s.get("user_id") for s in scans})
        vt_sum = sum(_flt(s.get("valor_total")) or _flt(s.get("valor_pagar")) for s in scans)
        vd_sum = sum(_flt(s.get("valor_desconto")) for s in scans)
        vp_sum = sum(_flt(s.get("valor_pagar")) for s in scans)

        # Products in period — collect scan_ids first
        period_ids = {s["scan_id"] for s in scans}
        all_prods = _load_csv_produtos()
        period_prods = [p for p in all_prods if p.get("scan_id") in period_ids]

        # Aggregate by (codigo, descricao), track lojas
        from collections import defaultdict
        prod_agg: dict[str, dict] = defaultdict(lambda: {"total_qtd": 0.0, "total_val": 0.0, "ocorrencias": 0, "descricao": "", "lojas": set()})
        for p in period_prods:
            key = p.get("codigo") or p.get("descricao", "?")
            prod_agg[key]["descricao"] = p.get("descricao") or key
            prod_agg[key]["total_qtd"] += _flt(p.get("quantidade"))
            prod_agg[key]["total_val"] += _flt(p.get("valor_total"))
            prod_agg[key]["ocorrencias"] += 1
            lid = p.get("loja_id") or ""
            if lid:
                prod_agg[key]["lojas"].add(lid)
        prods_raw = sorted(prod_agg.values(), key=lambda x: x["total_val"], reverse=True)[:15]

        # Payment breakdown — dedup by method name
        pag_agg: dict = defaultdict(lambda: {"total": 0.0, "cnt": 0})
        for s in scans:
            fp = s.get("forma_pagamento")
            if not fp:
                continue
            for part in fp.split(" | "):
                part = part.strip()
                pm2 = re.match(r"(.+?):\s*([\d,\.]+)$", part)
                if pm2:
                    meth   = pm2.group(1).strip()
                    amount = _flt(pm2.group(2).replace(",", "."))
                    pag_agg[meth]["total"] += amount
                    pag_agg[meth]["cnt"]   += 1
                else:
                    rv = re.match(r"R\$\s*([\d,\.]+)$", part)
                    meth = "Valor pago"
                    pag_agg[meth]["total"] += _flt(rv.group(1).replace(",", ".")) if rv else 0
                    pag_agg[meth]["cnt"]   += 1
        pag_raw = sorted(pag_agg.items(), key=lambda x: x[1]["total"], reverse=True)

        # Top emitentes — fallback valor_pagar when valor_total is 0
        emit_agg: dict[str, dict] = defaultdict(lambda: {"emitente_nome": "", "loja_id": "", "cnt": 0, "vt": 0.0})
        for s in scans:
            cnpj = s.get("cnpj_emit") or s.get("emitente_nome") or "?"
            emit_agg[cnpj]["emitente_nome"] = s.get("emitente_nome") or cnpj
            emit_agg[cnpj]["loja_id"] = s.get("loja_id", "")
            emit_agg[cnpj]["cnt"] += 1
            vt = _flt(s.get("valor_total")) or _flt(s.get("valor_pagar"))
            emit_agg[cnpj]["vt"] += vt
        emit_raw = sorted(emit_agg.values(), key=lambda x: x["cnt"], reverse=True)[:5]

    except Exception as exc:
        await message.reply_text(f"❌ Erro ao gerar relatório: {exc}"); return

    def _brl(v: float) -> str:
        return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")

    lines = [
        "╔══════════════════════════════════╗",
        f"║  📊  RELATÓRIO — {periodo_label:^17}║",
        "╚══════════════════════════════════╝",
        "",
        f"🧾  NFs escaneadas    : {total_scans}",
        f"👥  Usuários ativos   : {total_users}",
        f"💵  Total gasto       : {_brl(vt_sum)}",
        f"🏷️  Total descontos   : {_brl(vd_sum)}",
        f"💳  Total pago        : {_brl(vp_sum)}",
    ]

    if emit_raw:
        lines += ["", "────────────────────────────────────",
                  "🏪  Estabelecimentos mais frequentes",
                  "────────────────────────────────────"]
        for e in emit_raw:
            nome    = e["emitente_nome"] or "—"
            vt      = _brl(e["vt"] or 0)
            lid_tag = f" [{e['loja_id']}]" if e.get("loja_id") else ""
            lines.append(f"  🏬 {nome[:30]}{lid_tag}  ({e['cnt']}x) — {vt}")

    if pag_raw:
        lines += ["", "────────────────────────────────────",
                  "💳  Formas de pagamento",
                  "────────────────────────────────────"]
        for forma, data in pag_raw:
            lines.append(f"  • {str(forma)[:40]}: {_brl(data['total'])}  ({data['cnt']}x)")

    if prods_raw:
        lines += ["", "────────────────────────────────────",
                  "🛒  Produtos mais comprados (por valor)",
                  "────────────────────────────────────"]
        for i, p in enumerate(prods_raw, 1):
            desc  = (p["descricao"] or "—")[:35]
            qtd   = p["total_qtd"] or 0
            val   = _brl(p["total_val"] or 0)
            loja_tags = "/".join(sorted(p.get("lojas", set())))
            loja_str  = f" [{loja_tags}]" if loja_tags else ""
            lines.append(f"  {i:2}. {desc}{loja_str}")
            lines.append(f"      Qtd: {qtd:g}  Total: {val}  ({p['ocorrencias']}x)")

    lines += ["", f"🕒  Gerado em: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"]

    text = "\n".join(lines)
    if len(text) > 3950:
        text = text[:3950] + "\n…(truncado)"
    await message.reply_text(f"📝 [TEXTO] Relatório:\n\n{text}")


# ──────────────────────────────────────────────
# Error handler
# ──────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    from telegram.error import Conflict
    if isinstance(context.error, Conflict):
        logger.warning("409 Conflict: feche outras instâncias do bot.")
        return
    logger.exception("Erro não tratado:", exc_info=context.error)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN não encontrado no .env")

    _load_roles()
    _load_activity()

    app = Application.builder().token(BOT_TOKEN).build()

    # ── Passive activity tracker (runs on every message) ──────────────────
    app.add_handler(MessageHandler(filters.ALL, track_activity), group=-1)

    # ── Commands ──────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",      start_handler))
    app.add_handler(CommandHandler("help",        help_handler))
    app.add_handler(CommandHandler("myid",        myid_handler))
    app.add_handler(CommandHandler("nf",          nf_handler))
    app.add_handler(CommandHandler("addrole",     addrole_handler))
    app.add_handler(CommandHandler("removerole",  removerole_handler))
    app.add_handler(CommandHandler("logs",        logs_handler))
    app.add_handler(CommandHandler("resume",      resume_handler))
    app.add_handler(CommandHandler("users",       users_handler))

    # Photo with /nf caption (Telegram sends as message, not command)
    app.add_handler(MessageHandler(
        filters.PHOTO & filters.CaptionRegex(r"(?i)^/nf"),
        nf_handler,
    ))
    app.add_error_handler(error_handler)

    logger.info("Bot iniciado. Aguardando mensagens…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
