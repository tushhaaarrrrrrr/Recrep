from __future__ import annotations

import argparse
import asyncio
import hashlib
import html as html_lib
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

_HERE = Path(__file__).resolve().parent
for _parent in [_HERE, *_HERE.parents]:
    _dotenv = _parent / ".env"
    if _dotenv.exists():
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(_dotenv, override=False)
            print(f"[info] Loaded .env from {_dotenv}")
        except ImportError:
            print("[warn] python-dotenv not installed; skipping .env auto-load")
        break

try:
    from database.connection import get_db_pool, init_db_pool
    _USE_PROJECT_POOL = True
except ImportError:
    try:
        import asyncpg  # type: ignore
        _USE_PROJECT_POOL = False
        _DATABASE_URL = os.environ.get("DATABASE_URL")
        if not _DATABASE_URL:
            sys.exit("[error] DATABASE_URL is not set.")
    except ImportError:
        sys.exit("[error] asyncpg not installed. Run: pip install asyncpg")

try:
    import asyncpg  # type: ignore
except Exception:
    asyncpg = None  # type: ignore

MENTION_RE = re.compile(r"<@!?([0-9]+)>|@([0-9]{5,})")
CURRENCY = ""


def esc(value: Any) -> str:
    return html_lib.escape("" if value is None else str(value), quote=True)


def txt(value: Any) -> str:
    return "" if value is None else str(value)


def money(value: Any) -> str:
    try:
        return f"{CURRENCY}{float(value):,.0f}"
    except Exception:
        return "—"


def money2(value: Any) -> str:
    try:
        return f"{CURRENCY}{float(value):,.2f}"
    except Exception:
        return "—"


def fmt_date(value: Any) -> str:
    if not value:
        return "—"
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %H:%M")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")
    return txt(value)


def bool_word(value: Any) -> str:
    return "Yes" if bool(value) else "No"


def normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in re.split(r"[\n,|]+", s) if p.strip()]


def screenshot_urls(value: Any) -> list[str]:
    return normalize_list(value)


def display_name_for_id(discord_id: Any, staff_map: dict[Any, str]) -> str:
    return staff_map.get(discord_id, f"Staff #{str(discord_id)[-4:]}")


def resolve_mentions(value: Any, staff_map: dict[Any, str]) -> str:
    s = txt(value).strip()
    if not s:
        return "—"

    def repl(match: re.Match[str]) -> str:
        raw = match.group(1) or match.group(2)
        if raw is None:
            return match.group(0)
        try:
            return display_name_for_id(int(raw), staff_map)
        except Exception:
            return match.group(0)

    return MENTION_RE.sub(repl, s)


def safe_filename(prefix: str, url: str, fallback_ext: str = ".jpg") -> str:
    parsed = urlparse(url)
    base = Path(parsed.path).name
    ext = Path(base).suffix.lower() if Path(base).suffix else ""
    if ext not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff"}:
        ext = fallback_ext
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    safe_prefix = re.sub(r"[^a-zA-Z0-9_-]+", "_", prefix).strip("_") or "img"
    return f"{safe_prefix}_{digest}{ext}"


def download_url(url: str, out_dir: Path, prefix: str) -> tuple[str, bool]:
    if not url:
        return "", False
    if url.startswith("data:") or not url.startswith(("http://", "https://")):
        return url, False

    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / safe_filename(prefix, url)
    if dest.exists() and dest.stat().st_size > 0:
        return str(dest), True

    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*,*/*;q=0.8"})
    try:
        with urlopen(req, timeout=30) as response:
            data = response.read()
        if not data:
            return url, False
        dest.write_bytes(data)
        return str(dest), True
    except Exception:
        return url, False


def parse_hours(value: Any) -> float:
    """
    Robustly parse time-spent strings into decimal hours.
    Handles:
      - Unix timestamp ranges:  <t:1775462400:s> to <t:1775480400:s>
      - Explicit hour/minute mentions: "3 hours", "90 minutes", "1h 30m"
      - Parenthetical breakdown: extracts ONLY the direct hours, ignores AFK/material-grinding labels
      - AM/PM ranges: "10pm to 4:30am IST"
      - Falls back to 0.0 if nothing parseable
    """
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0

    # --- Unix timestamp pairs ---
    ts = re.findall(r"<t:(\d+)(?::[a-zA-Z])?>", s)
    if len(ts) >= 2:
        try:
            return abs(int(ts[1]) - int(ts[0])) / 3600.0
        except Exception:
            pass

    # --- AM/PM clock range (handles cross-midnight) ---
    m = re.search(
        r"(\d{1,2})(?:[:.](\d{2}))?\s*(am|pm).{0,20}?(\d{1,2})(?:[:.](\d{2}))?\s*(am|pm)",
        s,
        re.IGNORECASE,
    )
    if m:
        h1, min1, p1, h2, min2, p2 = m.groups()
        h1, h2 = int(h1), int(h2)
        min1, min2 = int(min1 or 0), int(min2 or 0)
        p1, p2 = p1.lower(), p2.lower()
        if p1 == "pm" and h1 != 12:
            h1 += 12
        if p1 == "am" and h1 == 12:
            h1 = 0
        if p2 == "pm" and h2 != 12:
            h2 += 12
        if p2 == "am" and h2 == 12:
            h2 = 0
        start_m = h1 * 60 + min1
        end_m = h2 * 60 + min2
        if end_m <= start_m:          # cross-midnight
            end_m += 24 * 60
        return (end_m - start_m) / 60.0

    # --- Explicit h/m mentions (NOT inside parenthetical side-notes) ---
    # Strategy: strip parenthetical sub-clauses that describe auxiliary time
    # e.g. "(1 hour for material grinding)" — we DO NOT count those separately
    # because they're already part of the stated total.
    # We only sum hours/minutes that appear OUTSIDE of parentheses OR that are
    # the ONLY number present.
    outside = re.sub(r"\([^)]*\)", " ", s)   # remove parenthetical content
    total_h = 0.0
    found = False
    for m2 in re.finditer(r"(\d+(?:\.\d+)?)\s*h(?:ours?)?", outside, re.IGNORECASE):
        total_h += float(m2.group(1))
        found = True
    for m2 in re.finditer(r"(\d+(?:\.\d+)?)\s*m(?:in(?:ute)?s?)?", outside, re.IGNORECASE):
        total_h += float(m2.group(1)) / 60.0
        found = True
    if found:
        return total_h

    # Fallback: try raw string with parentheses (old behaviour)
    slow = s.lower()
    total_f = 0.0
    for m2 in re.finditer(r"(\d+(?:\.\d+)?)\s*h(?:ours?)?", slow):
        total_f += float(m2.group(1))
    for m2 in re.finditer(r"(\d+(?:\.\d+)?)\s*m(?:in(?:ute)?s?)?", slow):
        total_f += float(m2.group(1)) / 60.0
    return total_f


def build_kind(project_name: str, time_spent: str) -> str:
    s = f"{project_name} {time_spent}".lower()
    if "road" in s:
        return "roads"
    if "plot" in s:
        return "plots"
    return "complex"


def build_rate(kind: str) -> int:
    return {"complex": 10000, "roads": 8000, "plots": 6000}.get(kind, 10000)


def recruitment_bonus(count: int, month_label: str) -> int:
    if count >= 100:
        return 5000000 if month_label.lower().startswith("april ") else 2000000
    if count >= 50:
        return 1000000
    return 0


def invoice_rate(count: int) -> float:
    return min(0.10 + 0.02 * (count // 5), 0.20)


def scroll_rate(scroll_type: str) -> int:
    return {
        "common": 5000,
        "special": 10000,
        "epic": 15000,
        "mythic": 25000,
        "legendary": 50000,
    }.get((scroll_type or "").strip().lower(), 0)


async def _fetch(query: str, *args):
    if _USE_PROJECT_POOL:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)
    async with _standalone_pool.acquire() as conn:  # type: ignore[name-defined]
        return await conn.fetch(query, *args)


async def get_staff():
    return await _fetch("""
        SELECT discord_id, display_name
        FROM staff_member
        ORDER BY COALESCE(display_name, discord_id::TEXT)
    """)


async def get_recruitments(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               ingame_username, discord_username, nickname, plots
        FROM recruitment
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


async def get_progress_reports(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               project_name, time_spent, helper_mentions, screenshot_urls
        FROM progress_report
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


async def get_purchase_invoices(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               purchasee_nickname, purchasee_ingame,
               purchase_type, num_plots, total_plots,
               amount_deposited, screenshot_urls
        FROM purchase_invoice
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


async def get_demolition_reports(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               ingame_username, removed, stashed_items, screenshot_urls
        FROM demolition_report
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


async def get_eviction_reports(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               ingame_owner, inactivity_period, items_stored, screenshot_urls
        FROM eviction_report
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


async def get_scroll_completions(start: date, end: date):
    return await _fetch("""
        SELECT submitted_by, id, submitted_at,
               scroll_type, items_stored, screenshot_urls
        FROM scroll_completion
        WHERE submitted_at >= $1 AND submitted_at < $2
          AND status = 'approved'
        ORDER BY submitted_by, submitted_at
    """, start, end)


@dataclass
class ReportRecord:
    category: str
    staff_id: Any
    staff_name: str
    record_id: Any
    submitted_at: datetime
    fields: dict[str, Any]        # display fields only (no __ keys)
    auto_raw: int
    final_raw: int
    is_editable: bool
    screenshots: list[str]


@dataclass
class ReportBundle:
    month_label: str
    start: date
    end: date
    staff: list[Any]
    staff_map: dict[Any, str]
    active_staff: list[Any]
    counts: dict[Any, dict[str, int]]
    total_records: int
    total_screenshots: int
    records: list[ReportRecord]
    assets_dir: Path


def _group_by_staff(records: Iterable[Any]) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = defaultdict(list)
    for record in records:
        grouped[record["submitted_by"]].append(record)
    return grouped


def _build_counts(staff: list[Any], grouped: dict[str, dict[Any, list[Any]]]) -> dict[Any, dict[str, int]]:
    counts: dict[Any, dict[str, int]] = {}
    for member in staff:
        did = member["discord_id"]
        counts[did] = {
            "recruitments": len(grouped["recruitments"].get(did, [])),
            "progress_reports": len(grouped["progress_reports"].get(did, [])),
            "purchase_invoices": len(grouped["purchase_invoices"].get(did, [])),
            "demolition_reports": len(grouped["demolition_reports"].get(did, [])),
            "eviction_reports": len(grouped["eviction_reports"].get(did, [])),
            "scroll_completions": len(grouped["scroll_completions"].get(did, [])),
        }
    return counts


def _active_staff_only(staff: list[Any], counts: dict[Any, dict[str, int]]) -> list[Any]:
    return [member for member in staff if any(counts.get(member["discord_id"], {}).values())]


async def load_bundle(month_str: str, output_path: Path, skip_empty: bool = False) -> ReportBundle:
    try:
        report_date = datetime.strptime(month_str, "%Y-%m")
    except ValueError:
        raise ValueError(f"Invalid month format '{month_str}'. Expected YYYY-MM.")

    year, month = report_date.year, report_date.month
    last_day = monthrange(year, month)[1]
    start = date(year, month, 1)
    end = date(year, month, last_day) + timedelta(days=1)
    month_label = report_date.strftime("%B %Y")

    global _standalone_pool
    _standalone_pool = None
    if not _USE_PROJECT_POOL:
        print("Connecting to database (standalone pool)…")
        _standalone_pool = await asyncpg.create_pool(  # type: ignore[attr-defined]
            _DATABASE_URL,
            min_size=1,
            max_size=3,
            command_timeout=60,
            statement_cache_size=0,
        )
    else:
        await init_db_pool()

    print(f"Fetching data for {month_label}…")
    staff = await get_staff()
    recruitments = await get_recruitments(start, end)
    progress = await get_progress_reports(start, end)
    invoices = await get_purchase_invoices(start, end)
    demolitions = await get_demolition_reports(start, end)
    evictions = await get_eviction_reports(start, end)
    scrolls = await get_scroll_completions(start, end)

    if _standalone_pool:
        await _standalone_pool.close()

    staff_map = {m["discord_id"]: (m["display_name"] or str(m["discord_id"])) for m in staff}
    grouped = {
        "recruitments": _group_by_staff(recruitments),
        "progress_reports": _group_by_staff(progress),
        "purchase_invoices": _group_by_staff(invoices),
        "demolition_reports": _group_by_staff(demolitions),
        "eviction_reports": _group_by_staff(evictions),
        "scroll_completions": _group_by_staff(scrolls),
    }
    counts = _build_counts(staff, grouped)
    active_staff = staff if not skip_empty else _active_staff_only(staff, counts)

    assets_dir = output_path.with_suffix("").with_name(output_path.stem + "_assets")
    assets_dir.mkdir(parents=True, exist_ok=True)

    records: list[ReportRecord] = []
    total_screenshots = 0

    def add_record(
        category: str,
        staff_id: Any,
        submitted_at: datetime,
        record_id: Any,
        fields: dict[str, Any],
        screenshots: list[str],
        auto_raw: int,
        final_raw: int,
        is_editable: bool = False,
    ):
        nonlocal total_screenshots
        staff_name = staff_map.get(staff_id, str(staff_id))
        total_screenshots += len(screenshots)
        records.append(
            ReportRecord(
                category=category,
                staff_id=staff_id,
                staff_name=staff_name,
                record_id=record_id,
                submitted_at=submitted_at,
                fields=fields,
                auto_raw=auto_raw,
                final_raw=final_raw,
                is_editable=is_editable,
                screenshots=screenshots,
            )
        )

    # ── Recruitments: one aggregated card per staff member ──────────────
    for did, recs in grouped["recruitments"].items():
        count = len(recs)
        auto = count * 7000
        add_record(
            "Recruitments",
            did,
            max(r["submitted_at"] for r in recs),
            f"recruit-{did}",
            {
                "Entries": str(count),
                "Rate": f"{money(7000)} / recruit",
                "Auto Payment": money(auto),
            },
            [],
            auto_raw=auto,
            final_raw=auto,
        )

    # ── Building: one card per progress report ───────────────────────────
    for row in progress:
        hours = parse_hours(row["time_spent"])
        kind = build_kind(txt(row["project_name"]), txt(row["time_spent"]))
        rate = build_rate(kind)
        auto = round(rate * hours * (1 + hours * 0.01))
        add_record(
            "Building",
            row["submitted_by"],
            row["submitted_at"],
            row["id"],
            {
                "Project": txt(row["project_name"]) or "—",
                "Time Spent": txt(row["time_spent"]) or "—",
                "Hours": f"{hours:.2f}",
                "Type": kind.title(),
                "Auto Payment": money(auto),
            },
            screenshot_urls(row["screenshot_urls"]),
            auto_raw=auto,
            final_raw=auto,
            is_editable=True,
        )

    # ── Invoices: one aggregated card per staff member ───────────────────
    for did, recs in grouped["purchase_invoices"].items():
        count = len(recs)
        gross = sum(float(r["amount_deposited"] or 0) for r in recs)
        rate = invoice_rate(count)
        auto = round(gross * rate)
        add_record(
            "Invoices",
            did,
            max(r["submitted_at"] for r in recs),
            f"invoice-{did}",
            {
                "Invoices": str(count),
                "Gross Amount": money2(gross),
                "Rate": f"{rate * 100:.0f}%",
                "Auto Payment": money(auto),
            },
            [],
            auto_raw=auto,
            final_raw=auto,
        )

    # ── Demolitions: one card per record ─────────────────────────────────
    for row in demolitions:
        auto = 4000
        add_record(
            "Demolitions",
            row["submitted_by"],
            row["submitted_at"],
            row["id"],
            {
                "Player": txt(row["ingame_username"]) or "—",
                "Removed": txt(row["removed"]) or "—",
                "Items Stashed": bool_word(row["stashed_items"]),
                "Auto Payment": money(auto),
            },
            screenshot_urls(row["screenshot_urls"]),
            auto_raw=auto,
            final_raw=auto,
            is_editable=True,
        )

    # ── Evictions: one aggregated card per staff member ──────────────────
    for did, recs in grouped["eviction_reports"].items():
        count = len(recs)
        auto = count * 2000
        add_record(
            "Evictions",
            did,
            max(r["submitted_at"] for r in recs),
            f"evict-{did}",
            {
                "Entries": str(count),
                "Rate": f"{money(2000)} / eviction",
                "Auto Payment": money(auto),
            },
            [],
            auto_raw=auto,
            final_raw=auto,
        )

    # ── Scrolls: one aggregated card per staff member ────────────────────
    for did, recs in grouped["scroll_completions"].items():
        by_type: dict[str, int] = defaultdict(int)
        for r in recs:
            by_type[(r["scroll_type"] or "Unknown").strip().title()] += 1
        auto = sum(scroll_rate(r["scroll_type"]) for r in recs)
        breakdown = " · ".join(f"{k} ×{v}" for k, v in sorted(by_type.items()))
        add_record(
            "Scrolls",
            did,
            max(r["submitted_at"] for r in recs),
            f"scroll-{did}",
            {
                "Total": str(len(recs)),
                "Breakdown": breakdown or "—",
                "Auto Payment": money(auto),
            },
            [],
            auto_raw=auto,
            final_raw=auto,
        )

    total_records = (
        len(list(recruitments)) + len(list(progress)) + len(list(invoices))
        + len(list(demolitions)) + len(list(evictions)) + len(list(scrolls))
    )

    # Download screenshots locally
    for rec in records:
        if not rec.screenshots:
            continue
        saved: list[str] = []
        for idx, url in enumerate(rec.screenshots, start=1):
            local_path, downloaded = download_url(url, assets_dir, f"{rec.category}_{rec.record_id}_{idx}")
            saved.append(
                os.path.relpath(local_path, start=output_path.parent) if downloaded else url
            )
        rec.screenshots = saved

    return ReportBundle(
        month_label=month_label,
        start=start,
        end=end,
        staff=list(staff),
        staff_map=staff_map,
        active_staff=active_staff,
        counts=counts,
        total_records=total_records,
        total_screenshots=total_screenshots,
        records=records,
        assets_dir=assets_dir,
    )


# ═══════════════════════════════════════════════════════════════
#  HTML RENDERING
# ═══════════════════════════════════════════════════════════════

CAT_META = {
    "Recruitments": {"icon": "⚔️", "color": "#a78bfa", "glow": "rgba(167,139,250,.25)"},
    "Building":     {"icon": "🏗️", "color": "#38bdf8", "glow": "rgba(56,189,248,.25)"},
    "Invoices":     {"icon": "📜", "color": "#34d399", "glow": "rgba(52,211,153,.25)"},
    "Demolitions":  {"icon": "💥", "color": "#fb923c", "glow": "rgba(251,146,60,.25)"},
    "Evictions":    {"icon": "🚪", "color": "#f87171", "glow": "rgba(248,113,113,.25)"},
    "Scrolls":      {"icon": "✨", "color": "#c084fc", "glow": "rgba(192,132,252,.25)"},
}

CATEGORIES = list(CAT_META.keys())


def _calc_cat_totals(bundle: ReportBundle) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for cat in CATEGORIES:
        rows = [r for r in bundle.records if r.category == cat]
        out[cat] = {
            "count": len(rows),
            "auto": sum(r.auto_raw for r in rows),
            "final": sum(r.final_raw for r in rows),
        }
    return out


def _render_record_card(rec: ReportRecord) -> str:
    # Build fields rows (no internal __ keys)
    field_rows = ""
    for label, val in rec.fields.items():
        field_rows += f'''
          <div class="field-row">
            <span class="fl">{esc(label)}</span>
            <span class="fv">{esc(val) if val not in (None, "") else "—"}</span>
          </div>'''

    finance_block = f'''
        <div class="finance-block static">
          <div class="fin-col">
            <span class="fin-label">Payment</span>
            <span class="fin-static">{esc(money(rec.final_raw))}</span>
          </div>
        </div>'''

    # Evidence screenshots
    shots = ""
    if rec.screenshots:
        items = "".join(
            f'<button class="shot" type="button" data-full="{esc(s)}">'
            f'<img src="{esc(s)}" alt="Evidence {i}" loading="lazy">'
            f'<span class="shot-label">Evidence {i}</span></button>'
            for i, s in enumerate(rec.screenshots, 1)
        )
        shots = f'<div class="evidence-block"><div class="evidence-label">Evidence</div><div class="shot-grid">{items}</div></div>'

    badge = "Verified" if rec.is_editable else "Auto"
    badge_cls = "badge-verified" if rec.is_editable else "badge-auto"
    search_str = esc(
        " ".join([rec.staff_name, rec.category, str(rec.record_id)] + list(rec.fields.values())).lower()
    )

    return f'''
    <article class="record-card" 
      data-category="{esc(rec.category)}" 
      data-staff="{esc(str(rec.staff_id))}"
      data-search="{search_str}"
      data-key="{esc(rec.category)}::{esc(rec.record_id)}"
      data-auto="{rec.auto_raw}">
      <div class="card-header">
        <div class="card-meta">
          <span class="card-name">{esc(rec.staff_name)}</span>
          <span class="card-date">{esc(fmt_date(rec.submitted_at))}</span>
        </div>
        <span class="badge {badge_cls}">{badge}</span>
      </div>
      <div class="fields-block">{field_rows}</div>
      {finance_block}
      {shots}
    </article>'''


def _render_staff_card(staff_id: Any, staff_name: str, records: list[ReportRecord]) -> str:
    """Render a staff-member summary card showing per-category breakdown."""
    total_auto = sum(r.auto_raw for r in records)
    total_final = sum(r.final_raw for r in records)

    cat_rows = ""
    for cat in CATEGORIES:
        cat_recs = [r for r in records if r.category == cat]
        if not cat_recs:
            continue
        meta = CAT_META[cat]
        cat_auto = sum(r.auto_raw for r in cat_recs)
        cat_rows += f'''
        <div class="sc-row">
          <span class="sc-cat" style="color:{meta['color']}">{meta['icon']} {esc(cat)}</span>
          <span class="sc-entries">{len(cat_recs)} entries</span>
          <span class="sc-amt" data-staff-cat="{esc(str(staff_id))}:{esc(cat)}">{esc(money(cat_auto))}</span>
        </div>'''

    return f'''
    <div class="staff-card" data-staff-id="{esc(str(staff_id))}">
      <div class="sc-head">
        <div class="sc-avatar">{esc(staff_name[:2].upper())}</div>
        <div>
          <div class="sc-name">{esc(staff_name)}</div>
          <div class="sc-sub">{len(records)} submission{"s" if len(records) != 1 else ""}</div>
        </div>
        <div class="sc-total" data-staff-total="{esc(str(staff_id))}">{esc(money(total_final))}</div>
      </div>
      <div class="sc-breakdown">{cat_rows}</div>
    </div>'''


def render_html(bundle: ReportBundle) -> str:
    cat_totals = _calc_cat_totals(bundle)
    grand_auto = sum(v["auto"] for v in cat_totals.values())
    grand_final = sum(v["final"] for v in cat_totals.values())

    # ── Category view sections ───────────────────────────────────────────
    cat_sections = ""
    for cat in CATEGORIES:
        meta = CAT_META[cat]
        rows = [r for r in bundle.records if r.category == cat]
        ct = cat_totals[cat]
        cards = "".join(_render_record_card(r) for r in rows) if rows else \
            '<div class="empty-state">No approved submissions this period.</div>'
        cat_sections += f'''
      <section class="cat-section" data-section="{esc(cat)}" style="--cat-color:{meta['color']};--cat-glow:{meta['glow']}">
        <div class="cat-header">
          <div class="cat-title-group">
            <span class="cat-icon">{meta['icon']}</span>
            <div>
              <h2 class="cat-title">{esc(cat)}</h2>
              <p class="cat-sub">{ct['count']:,} records · Auto {esc(money(ct['auto']))}</p>
            </div>
          </div>
          <div class="cat-total-area">
            <div class="cat-final-pill" data-cat-final="{esc(cat)}">{esc(money(ct['final']))}</div>
          </div>
        </div>
        <div class="records-grid">{cards}</div>
      </section>'''

    # ── Staff summary view ───────────────────────────────────────────────
    staff_by_id: dict[Any, list[ReportRecord]] = defaultdict(list)
    for rec in bundle.records:
        staff_by_id[rec.staff_id].append(rec)

    staff_cards = ""
    for member in bundle.active_staff:
        did = member["discord_id"]
        name = member["display_name"] or str(did)
        recs = staff_by_id.get(did, [])
        if recs:
            staff_cards += _render_staff_card(did, name, recs)

    if not staff_cards:
        staff_cards = '<div class="empty-state">No active staff this period.</div>'

    # ── KPI pills ────────────────────────────────────────────────────────
    cat_pills = ""
    for cat in CATEGORIES:
        meta = CAT_META[cat]
        ct = cat_totals[cat]
        cat_pills += f'''
        <div class="kpi-pill" data-filter="{esc(cat)}" style="--pc:{meta['color']};--pg:{meta['glow']}">
          <span class="kp-icon">{meta['icon']}</span>
          <span class="kp-label">{esc(cat)}</span>
          <span class="kp-count">{ct['count']:,}</span>
          <span class="kp-amt" data-kpi-cat="{esc(cat)}">{esc(money(ct['final']))}</span>
        </div>'''

    generated_at = fmt_date(datetime.now(timezone.utc)) + " UTC"
    top_name = bundle.active_staff[0]["display_name"] if bundle.active_staff else "—"

    return f'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Payment Ledger · {esc(bundle.month_label)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@400;500&family=Instrument+Serif:ital@0;1&display=swap" rel="stylesheet" />
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
  <style>
/* ── Reset & base ─────────────────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
:root {{
  --bg: #08090d;
  --surface: #0e1018;
  --surface2: #12141e;
  --border: rgba(255,255,255,.07);
  --border2: rgba(255,255,255,.12);
  --text: #e8eaf2;
  --muted: #6b7494;
  --muted2: #9aa0c0;
  --green: #34d399;
  --green-dim: rgba(52,211,153,.15);
  --shadow-lg: 0 32px 64px rgba(0,0,0,.5);
  --shadow-sm: 0 4px 20px rgba(0,0,0,.3);
  --radius: 16px;
  --radius-sm: 10px;
  --font-display: 'Syne', sans-serif;
  --font-body: 'DM Mono', monospace;
  --font-serif: 'Instrument Serif', serif;
}}
html {{ scroll-behavior: smooth; }}
body {{
  font-family: var(--font-body);
  background: var(--bg);
  color: var(--text);
  font-size: 14px;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
}}

/* ── Scrollbar ────────────────────────────────────────────────────────── */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: rgba(255,255,255,.12); border-radius: 99px; }}

/* ── Noise overlay ────────────────────────────────────────────────────── */
body::before {{
  content: '';
  position: fixed; inset: 0; z-index: 0; pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");
  background-size: 200px 200px;
  opacity: 0.6;
}}

/* ── Layout ───────────────────────────────────────────────────────────── */
.wrap {{ position: relative; z-index: 1; max-width: 1280px; margin: 0 auto; padding: 0 24px 80px; }}

/* ── Hero ─────────────────────────────────────────────────────────────── */
.hero {{
  padding: 60px 0 48px;
  border-bottom: 1px solid var(--border);
}}
.hero-eyebrow {{
  font-family: var(--font-body);
  font-size: 11px;
  letter-spacing: .18em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 20px;
  display: flex;
  align-items: center;
  gap: 10px;
}}
.hero-eyebrow::before {{
  content: '';
  display: inline-block;
  width: 24px; height: 1px;
  background: var(--muted);
}}
.hero-title {{
  font-family: var(--font-display);
  font-size: clamp(2.8rem, 6vw, 5.2rem);
  font-weight: 800;
  line-height: .95;
  letter-spacing: -.04em;
  margin-bottom: 6px;
}}
.hero-title em {{
  font-family: var(--font-serif);
  font-style: italic;
  font-weight: 400;
  color: var(--muted2);
}}
.hero-month {{
  font-family: var(--font-display);
  font-size: clamp(2.8rem, 6vw, 5.2rem);
  font-weight: 800;
  line-height: .95;
  letter-spacing: -.04em;
  background: linear-gradient(135deg, #e8eaf2 0%, #6b7494 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  display: block;
  margin-bottom: 28px;
}}
.hero-meta {{
  display: flex;
  gap: 32px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--muted);
  letter-spacing: .04em;
}}
.hero-meta span {{ display: flex; align-items: center; gap: 6px; }}
.hero-meta strong {{ color: var(--text); font-weight: 500; }}

/* ── Grand totals bar ─────────────────────────────────────────────────── */
.totals-bar {{
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1px;
  background: var(--border);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  overflow: hidden;
  margin: 32px 0;
}}
.total-cell {{
  background: var(--surface);
  padding: 20px 24px;
}}
.total-cell:first-child {{ border-radius: var(--radius) 0 0 var(--radius); }}
.total-cell:last-child {{ border-radius: 0 var(--radius) var(--radius) 0; }}
.tc-label {{
  font-size: 11px;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 8px;
}}
.tc-value {{
  font-family: var(--font-display);
  font-size: 1.9rem;
  font-weight: 700;
  letter-spacing: -.03em;
}}
.tc-value.green {{ color: var(--green); }}

/* ── KPI pills row ────────────────────────────────────────────────────── */
.kpi-row {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 32px;
}}
.kpi-pill {{
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 14px;
  border-radius: 99px;
  border: 1px solid var(--border2);
  background: rgba(255,255,255,.03);
  font-size: 12px;
  cursor: pointer;
  transition: all .15s;
  user-select: none;
}}
.kpi-pill:hover, .kpi-pill.active {{
  background: var(--pg, rgba(255,255,255,.08));
  border-color: var(--pc, rgba(255,255,255,.2));
}}
.kpi-pill.active {{ box-shadow: 0 0 0 1px var(--pc); }}
.kp-icon {{ font-size: 14px; }}
.kp-label {{ color: var(--muted2); }}
.kp-count {{
  background: rgba(255,255,255,.06);
  border-radius: 99px;
  padding: 1px 7px;
  font-size: 11px;
  color: var(--muted);
}}
.kp-amt {{
  font-family: var(--font-display);
  font-weight: 700;
  color: var(--pc, var(--text));
  font-size: 13px;
}}

/* ── Toolbar ──────────────────────────────────────────────────────────── */
.toolbar {{
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 24px;
}}
.search-wrap {{
  flex: 1 1 260px;
  display: flex;
  align-items: center;
  gap: 10px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius-sm);
  padding: 10px 14px;
}}
.search-wrap input {{
  background: transparent;
  border: none;
  outline: none;
  color: var(--text);
  font-family: var(--font-body);
  font-size: 13px;
  width: 100%;
}}
.search-wrap input::placeholder {{ color: var(--muted); }}
.search-icon {{ color: var(--muted); font-size: 15px; }}
.view-toggle {{
  display: flex;
  gap: 4px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius-sm);
  padding: 4px;
}}
.view-btn {{
  padding: 7px 16px;
  border-radius: 7px;
  border: none;
  background: transparent;
  color: var(--muted2);
  font-family: var(--font-body);
  font-size: 12px;
  letter-spacing: .05em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all .15s;
}}
.view-btn.active {{
  background: rgba(255,255,255,.1);
  color: var(--text);
}}
.export-btn {{
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 18px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border2);
  background: var(--surface);
  color: var(--text);
  font-family: var(--font-body);
  font-size: 12px;
  letter-spacing: .05em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all .15s;
}}
.export-btn:hover {{
  background: var(--surface2);
  border-color: var(--green);
  color: var(--green);
}}
.export-btn svg {{ flex-shrink: 0; }}

/* ── Category sections ────────────────────────────────────────────────── */
.cat-section {{
  margin-bottom: 48px;
  display: none;
}}
.cat-section.visible {{ display: block; }}
.cat-header {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  padding: 20px 0 16px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 20px;
  flex-wrap: wrap;
}}
.cat-title-group {{
  display: flex;
  align-items: center;
  gap: 14px;
}}
.cat-icon {{ font-size: 24px; }}
.cat-title {{
  font-family: var(--font-display);
  font-size: 1.3rem;
  font-weight: 700;
  letter-spacing: -.02em;
  color: var(--cat-color, var(--text));
}}
.cat-sub {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
.cat-total-area {{
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}}
.cat-final-pill {{
  padding: 7px 16px;
  border-radius: 99px;
  border: 1px solid var(--cat-color, var(--border2));
  background: var(--cat-glow, rgba(255,255,255,.04));
  font-family: var(--font-display);
  font-size: 1rem;
  font-weight: 700;
  color: var(--cat-color, var(--text));
  letter-spacing: -.02em;
  white-space: nowrap;
}}

/* ── Records grid ─────────────────────────────────────────────────────── */
.records-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: 12px;
}}

/* ── Record card ──────────────────────────────────────────────────────── */
.record-card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 16px;
  transition: border-color .15s, box-shadow .15s;
}}
.record-card:hover {{
  border-color: var(--border2);
  box-shadow: var(--shadow-sm);
}}
.card-header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 14px;
}}
.card-name {{
  font-family: var(--font-display);
  font-size: .95rem;
  font-weight: 700;
  display: block;
  margin-bottom: 2px;
}}
.card-date {{ font-size: 11px; color: var(--muted); }}
.badge {{
  font-size: 10px;
  letter-spacing: .1em;
  text-transform: uppercase;
  padding: 4px 10px;
  border-radius: 99px;
  border: 1px solid;
  flex-shrink: 0;
}}
.badge-verified {{
  border-color: rgba(56,189,248,.35);
  color: #38bdf8;
  background: rgba(56,189,248,.08);
}}
.badge-auto {{
  border-color: rgba(100,116,139,.35);
  color: var(--muted2);
  background: rgba(100,116,139,.08);
}}

/* ── Fields block ─────────────────────────────────────────────────────── */
.fields-block {{
  display: flex;
  flex-direction: column;
  gap: 0;
  margin-bottom: 12px;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}}
.field-row {{
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border);
}}
.field-row:last-child {{ border-bottom: none; }}
.fl {{
  font-size: 11px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--muted);
  flex-shrink: 0;
}}
.fv {{
  font-size: 13px;
  font-weight: 500;
  color: var(--text);
  text-align: right;
  word-break: break-word;
}}

/* ── Finance block ────────────────────────────────────────────────────── */
.finance-block {{
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}}
.fin-col {{
  flex: 1;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px 12px;
}}
.fin-label {{
  display: block;
  font-size: 10px;
  letter-spacing: .1em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 6px;
}}
.fin-input {{
  width: 100%;
  background: transparent;
  border: none;
  outline: none;
  color: var(--green);
  font-family: var(--font-display);
  font-size: 1rem;
  font-weight: 700;
  letter-spacing: -.02em;
}}
.fin-input::-webkit-inner-spin-button,
.fin-input::-webkit-outer-spin-button {{ opacity: .3; }}
.fin-static {{
  font-family: var(--font-display);
  font-size: 1rem;
  font-weight: 700;
  color: var(--text);
  letter-spacing: -.02em;
}}

/* ── Evidence ─────────────────────────────────────────────────────────── */
.evidence-block {{ margin-top: 4px; }}
.evidence-label {{
  font-size: 10px;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 8px;
}}
.shot-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
  gap: 6px;
}}
.shot {{
  all: unset;
  cursor: pointer;
  border-radius: var(--radius-sm);
  overflow: hidden;
  border: 1px solid var(--border);
  background: var(--surface2);
  display: block;
  transition: border-color .15s;
}}
.shot:hover {{ border-color: var(--border2); }}
.shot img {{ width: 100%; aspect-ratio: 16/10; object-fit: cover; display: block; }}
.shot-label {{ display: block; padding: 5px 8px; font-size: 10px; color: var(--muted2); }}

/* ── Staff view ───────────────────────────────────────────────────────── */
#staffView {{ display: none; }}
#staffView.visible {{ display: block; }}
.staff-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 12px;
}}
.staff-card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 16px;
  transition: border-color .15s;
}}
.staff-card:hover {{ border-color: var(--border2); }}
.sc-head {{
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
}}
.sc-avatar {{
  width: 40px; height: 40px;
  border-radius: 10px;
  background: var(--surface2);
  border: 1px solid var(--border2);
  display: flex;
  align-items: center;
  justify-content: center;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: .85rem;
  color: var(--muted2);
  flex-shrink: 0;
}}
.sc-name {{
  font-family: var(--font-display);
  font-weight: 700;
  font-size: .95rem;
}}
.sc-sub {{ font-size: 11px; color: var(--muted); }}
.sc-total {{
  margin-left: auto;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 1.1rem;
  color: var(--green);
  letter-spacing: -.03em;
}}
.sc-breakdown {{
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}}
.sc-row {{
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border);
  font-size: 12px;
}}
.sc-row:last-child {{ border-bottom: none; }}
.sc-cat {{ flex: 1; }}
.sc-entries {{ color: var(--muted); font-size: 11px; }}
.sc-amt {{
  font-family: var(--font-display);
  font-weight: 700;
  font-size: .88rem;
}}

/* ── Empty state ──────────────────────────────────────────────────────── */
.empty-state {{
  padding: 32px;
  text-align: center;
  color: var(--muted);
  border: 1px dashed var(--border2);
  border-radius: var(--radius);
  font-size: 13px;
}}

/* ── Lightbox ─────────────────────────────────────────────────────────── */
.lightbox {{
  position: fixed; inset: 0; z-index: 999;
  background: rgba(0,0,0,.88);
  display: none;
  place-items: center;
  padding: 20px;
  backdrop-filter: blur(8px);
}}
.lightbox.open {{ display: grid; }}
.lightbox-inner {{
  position: relative;
  width: min(1100px, 96vw);
  max-height: 92vh;
  border-radius: var(--radius);
  overflow: hidden;
  border: 1px solid var(--border2);
}}
.lightbox-inner img {{
  width: 100%;
  max-height: 90vh;
  object-fit: contain;
  display: block;
  background: #000;
}}
.lightbox-close {{
  position: absolute;
  top: 12px; right: 12px;
  all: unset;
  cursor: pointer;
  width: 32px; height: 32px;
  border-radius: 99px;
  background: rgba(0,0,0,.6);
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 18px;
  border: 1px solid rgba(255,255,255,.2);
  transition: background .15s;
}}
.lightbox-close:hover {{ background: rgba(255,255,255,.1); }}

/* ── Footer ───────────────────────────────────────────────────────────── */
.site-footer {{
  padding: 32px 0;
  border-top: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  font-size: 11px;
  color: var(--muted);
  letter-spacing: .06em;
}}
.site-footer code {{ color: var(--muted2); font-family: var(--font-body); }}

/* ── Divider ──────────────────────────────────────────────────────────── */
.section-divider {{ height: 1px; background: var(--border); margin: 32px 0; }}

/* ── PDF export adjustment ────────────────────────────────────────────── */
@media print {{
  .toolbar, .site-footer, .lightbox, .export-btn {{ display: none !important; }}
  .cat-section {{ display: block !important; page-break-inside: avoid; }}
  .records-grid {{ grid-template-columns: 1fr 1fr; }}
  body {{ background: #fff; color: #000; }}
  .record-card, .fields-block, .field-row, .finance-block, .fin-col {{
    border-color: #ddd !important;
    background: #f9f9f9 !important;
    color: #111 !important;
  }}
}}

/* ── Animations ───────────────────────────────────────────────────────── */
@keyframes fadeUp {{
  from {{ opacity: 0; transform: translateY(12px); }}
  to   {{ opacity: 1; transform: translateY(0); }}
}}
.cat-section.visible {{ animation: fadeUp .25s ease both; }}
.hero {{ animation: fadeUp .3s .05s ease both; }}
.totals-bar {{ animation: fadeUp .3s .1s ease both; opacity: 0; animation-fill-mode: forwards; }}
.kpi-row {{ animation: fadeUp .3s .15s ease both; opacity: 0; animation-fill-mode: forwards; }}

/* ── Responsive ───────────────────────────────────────────────────────── */
@media (max-width: 768px) {{
  .wrap {{ padding: 0 14px 60px; }}
  .totals-bar {{ grid-template-columns: 1fr; }}
  .total-cell:first-child {{ border-radius: var(--radius) var(--radius) 0 0; }}
  .total-cell:last-child {{ border-radius: 0 0 var(--radius) var(--radius); }}
  .cat-header {{ flex-direction: column; align-items: flex-start; }}
  .records-grid {{ grid-template-columns: 1fr; }}
}}
  </style>
</head>
<body>
<div class="wrap">

  <!-- ── Hero ─────────────────────────────────────────────────────────── -->
  <header class="hero">
    <div class="hero-eyebrow">Payment Ledger · {esc(bundle.month_label)}</div>
    <h1 class="hero-title">Staff <em>payouts</em></h1>
    <span class="hero-month">{esc(bundle.month_label)}</span>
    <div class="hero-meta">
      <span>Window <strong>{esc(bundle.start.strftime('%d %b'))} → {esc((bundle.end - timedelta(days=1)).strftime('%d %b %Y'))}</strong></span>
      <span>Records <strong>{bundle.total_records:,}</strong></span>
      <span>Active staff <strong>{len(bundle.active_staff):,}</strong></span>
      <span>Evidence <strong>{bundle.total_screenshots:,} files</strong></span>
      <span>Generated <strong>{esc(generated_at)}</strong></span>
    </div>
  </header>

  <!-- ── Totals bar ────────────────────────────────────────────────────── -->
  <div class="totals-bar">
    <div class="total-cell">
      <div class="tc-label">Auto Total</div>
      <div class="tc-value" id="grandAuto">{esc(money(grand_auto))}</div>
    </div>
    <div class="total-cell">
      <div class="tc-label">Payment Total</div>
      <div class="tc-value green" id="grandFinal">{esc(money(grand_final))}</div>
    </div>
  </div>

  <!-- ── KPI row ───────────────────────────────────────────────────────── -->
  <div class="kpi-row" id="kpiRow">
    <div class="kpi-pill active" data-filter="all" style="--pc:#e8eaf2;--pg:rgba(232,234,242,.08)">
      <span class="kp-icon">◎</span>
      <span class="kp-label">All</span>
      <span class="kp-count">{bundle.total_records:,}</span>
      <span class="kp-amt" id="kpiAllAmt">{esc(money(grand_final))}</span>
    </div>
    {cat_pills}
  </div>

  <!-- ── Toolbar ───────────────────────────────────────────────────────── -->
  <div class="toolbar">
    <div class="search-wrap">
      <span class="search-icon">⌕</span>
      <input id="searchInput" type="search" placeholder="Search staff, categories, amounts…" autocomplete="off" />
    </div>
    <div class="view-toggle">
      <button class="view-btn active" data-view="category" type="button">By Category</button>
      <button class="view-btn" data-view="staff" type="button">By Staff</button>
    </div>
    <button class="export-btn" id="exportBtn" type="button">
      <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
        <path d="M12 16V4m0 12-4-4m4 4 4-4M4 20h16"/>
      </svg>
      Export PDF
    </button>
  </div>

  <!-- ── Category view ─────────────────────────────────────────────────── -->
  <div id="categoryView">
    {cat_sections}
  </div>

  <!-- ── Staff view ────────────────────────────────────────────────────── -->
  <div id="staffView">
    <div class="staff-grid" id="staffGrid">
      {staff_cards}
    </div>
  </div>

  <!-- ── Footer ────────────────────────────────────────────────────────── -->
  <footer class="site-footer">
    <span>Evidence assets · <code>{esc(str(bundle.assets_dir))}</code></span>
    <span>Building &amp; Demolition rows are included as records · Payments are shown without bonuses</span>
  </footer>
</div>

<!-- ── Lightbox ──────────────────────────────────────────────────────── -->
<div class="lightbox" id="lightbox" aria-hidden="true">
  <div class="lightbox-inner">
    <img id="lightboxImg" src="" alt="Evidence screenshot" />
    <button class="lightbox-close" id="lightboxClose" type="button">✕</button>
  </div>
</div>

<script>

(function() {{
  'use strict';

  const categoryView = document.getElementById('categoryView');
  const staffView = document.getElementById('staffView');
  const kpiRow = document.getElementById('kpiRow');
  const searchInput = document.getElementById('searchInput');
  const exportBtn = document.getElementById('exportBtn');
  const viewButtons = document.querySelectorAll('.view-btn');

  let currentView = 'category';
  let activeFilter = 'all';

  function applyFilter() {{
    const query = searchInput.value.trim().toLowerCase();

    if (currentView === 'category') {{
      document.querySelectorAll('.cat-section').forEach(section => {{
        const cat = section.dataset.section;
        const show = activeFilter === 'all' || cat === activeFilter;
        section.classList.toggle('visible', show);
        if (!show) return;

        let anyVisible = false;
        section.querySelectorAll('.record-card').forEach(card => {{
          const ok = !query || (card.dataset.search || '').includes(query);
          card.style.display = ok ? '' : 'none';
          if (ok) anyVisible = true;
        }});

        const empty = section.querySelector('.empty-state');
        if (empty) empty.style.display = anyVisible ? 'none' : '';
      }});
    }} else {{
      document.querySelectorAll('.staff-card').forEach(card => {{
        const ok = !query || card.textContent.toLowerCase().includes(query);
        card.style.display = ok ? '' : 'none';
      }});
    }}
  }}

  function setView(view) {{
    currentView = view;
    viewButtons.forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
    if (view === 'category') {{
      categoryView.style.display = '';
      staffView.classList.remove('visible');
    }} else {{
      categoryView.style.display = 'none';
      staffView.classList.add('visible');
    }}
    applyFilter();
  }}

  kpiRow.addEventListener('click', e => {{
    const pill = e.target.closest('.kpi-pill');
    if (!pill) return;
    kpiRow.querySelectorAll('.kpi-pill').forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    activeFilter = pill.dataset.filter || 'all';
    setView('category');
  }});

  searchInput.addEventListener('input', applyFilter);

  viewButtons.forEach(btn => {{
    btn.addEventListener('click', () => setView(btn.dataset.view));
  }});

  const lightbox = document.getElementById('lightbox');
  const lightboxImg = document.getElementById('lightboxImg');
  const closeLightbox = () => {{
    lightbox.classList.remove('open');
    lightbox.setAttribute('aria-hidden', 'true');
    lightboxImg.src = '';
  }};

  document.addEventListener('click', e => {{
    const shot = e.target.closest('.shot');
    if (!shot) return;
    const src = shot.dataset.full || shot.querySelector('img')?.src;
    if (!src) return;
    lightboxImg.src = src;
    lightbox.classList.add('open');
    lightbox.setAttribute('aria-hidden', 'false');
  }});

  lightbox.addEventListener('click', e => {{
    if (e.target === lightbox) closeLightbox();
  }});
  document.getElementById('lightboxClose').addEventListener('click', closeLightbox);
  document.addEventListener('keydown', e => {{
    if (e.key === 'Escape') closeLightbox();
  }});

  function copyHeadStylesInto(targetRoot) {{
    // Copy all <style> tags from <head> so the cloned PDF root keeps the same layout rules.
    document.head.querySelectorAll('style').forEach(node => {{
      targetRoot.appendChild(node.cloneNode(true));
    }});
  }}

  async function exportPdf() {{
    const btn = exportBtn;
    const label = btn.innerHTML;
    btn.disabled = true;
    btn.textContent = 'Generating…';

    const exportRoot = document.createElement('div');
    exportRoot.id = 'pdfExportRoot';
    exportRoot.style.position = 'fixed';
    exportRoot.style.left = '-10000px';
    exportRoot.style.top = '0';
    exportRoot.style.width = '1280px';
    exportRoot.style.background = '#ffffff';
    exportRoot.style.color = '#111111';

    // Critical: bring the page's head styles into the PDF clone.
    copyHeadStylesInto(exportRoot);

    const pdfStyle = document.createElement('style');
    pdfStyle.textContent = `
      #pdfExportRoot, #pdfExportRoot * {{
        animation: none !important;
        transition: none !important;
      }}

      #pdfExportRoot .wrap {{
        max-width: 1280px !important;
      }}

      #pdfExportRoot .toolbar,
      #pdfExportRoot .site-footer,
      #pdfExportRoot .lightbox,
      #pdfExportRoot .view-toggle,
      #pdfExportRoot .search-wrap,
      #pdfExportRoot .export-btn {{
        display: none !important;
      }}

      #pdfExportRoot #staffView {{
        display: none !important;
      }}

      #pdfExportRoot #categoryView {{
        display: block !important;
      }}

      #pdfExportRoot .cat-section,
      #pdfExportRoot .cat-section.visible {{
        display: block !important;
        opacity: 1 !important;
        visibility: visible !important;
        break-inside: avoid;
        page-break-inside: avoid;
      }}

      #pdfExportRoot .records-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
      }}

      #pdfExportRoot .record-card,
      #pdfExportRoot .fields-block,
      #pdfExportRoot .field-row,
      #pdfExportRoot .finance-block,
      #pdfExportRoot .fin-col,
      #pdfExportRoot .totals-bar,
      #pdfExportRoot .kpi-pill {{
        border-color: #d7d7d7 !important;
        background: #ffffff !important;
        color: #111111 !important;
      }}

      #pdfExportRoot .hero-title,
      #pdfExportRoot .hero-month,
      #pdfExportRoot .tc-value,
      #pdfExportRoot .kp-amt,
      #pdfExportRoot .cat-final-pill,
      #pdfExportRoot .sc-total {{
        color: #111111 !important;
        -webkit-text-fill-color: #111111 !important;
      }}
    `;
    exportRoot.appendChild(pdfStyle);

    const source = document.querySelector('.wrap');
    const clone = source.cloneNode(true);

    clone.querySelector('.toolbar')?.remove();
    clone.querySelector('.site-footer')?.remove();
    clone.querySelector('.lightbox')?.remove();
    clone.querySelector('#staffView')?.remove();

    const pdfCategoryView = clone.querySelector('#categoryView');
    if (pdfCategoryView) pdfCategoryView.style.display = 'block';

    clone.querySelectorAll('.cat-section').forEach(section => {{
      section.classList.add('visible');
      section.style.display = 'block';
      section.style.pageBreakInside = 'avoid';
      section.style.breakInside = 'avoid';
    }});

    clone.querySelectorAll('*').forEach(el => {{
      el.style.animation = 'none';
      el.style.transition = 'none';
    }});

    exportRoot.appendChild(clone);
    document.body.appendChild(exportRoot);

    try {{
      if (document.fonts && document.fonts.ready) {{
        await document.fonts.ready;
      }}

      const opt = {{
        margin: [8, 8, 8, 8],
        filename: '{esc(bundle.month_label).replace(" ", "-")}.pdf',
        image: {{ type: 'jpeg', quality: 0.98 }},
        html2canvas: {{
          scale: 2,
          useCORS: true,
          logging: false,
          backgroundColor: '#ffffff',
          scrollY: 0,
          windowWidth: 1280,
        }},
        jsPDF: {{ unit: 'mm', format: 'a4', orientation: 'portrait' }},
        pagebreak: {{ mode: ['css', 'legacy'] }},
      }};

      await html2pdf().set(opt).from(exportRoot).save();
    }} catch (err) {{
      alert('PDF export failed. Try printing with Ctrl+P instead.');
    }} finally {{
      exportRoot.remove();
      btn.innerHTML = label;
      btn.disabled = false;
    }}
  }}

  exportBtn.addEventListener('click', exportPdf);

  document.querySelectorAll('.cat-section').forEach(s => s.classList.add('visible'));
  applyFilter();
}})();

</script>
</body>
</html>'''


async def generate_report(month_str: str, output_path: str, skip_empty: bool = False):
    output = Path(output_path).expanduser().resolve()
    bundle = await load_bundle(month_str, output, skip_empty=skip_empty)
    print("Rendering HTML report…")
    html = render_html(bundle)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    print(f"Report saved → {output}")
    print(f"  Active staff      : {len(bundle.active_staff)}")
    print(f"  Total records     : {bundle.total_records}")
    print(f"  Evidence assets   : {bundle.total_screenshots}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a minimal premium HTML monthly payment ledger."
    )
    parser.add_argument("--month", required=True, help="Month to report on, format YYYY-MM")
    parser.add_argument("--output", default=None, help="Output .html file path")
    parser.add_argument("--skip-empty", action="store_true", help="Omit staff with no activity")
    args = parser.parse_args()
    output = args.output or f"monthly_payment_report_{args.month}.html"
    try:
        asyncio.run(generate_report(args.month, output, skip_empty=args.skip_empty))
    except ValueError as exc:
        sys.exit(f"[error] {exc}")
    except KeyboardInterrupt:
        sys.exit("Aborted.")


if __name__ == "__main__":
    main()