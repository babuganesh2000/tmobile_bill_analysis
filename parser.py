"""
T-Mobile PDF bill parser & DuckDB loader.
Shared module used by both the CLI tool (load_bills.py) and the Streamlit app.
"""

import re, io
import duckdb
import pdfplumber
from datetime import datetime

# ─── helpers ────────────────────────────────────────────────────────

def _pm(text: str) -> float:
    """Parse money: '$267.57' -> 267.57; '-$38.00' -> -38.0; '-' -> 0.0"""
    if not text or text.strip() in ("-", ""):
        return 0.0
    t = text.strip().replace(",", "")
    neg = t.startswith("-")
    m = re.search(r"\$?([\d]+\.?\d*)", t)
    if m:
        v = float(m.group(1))
        return -v if neg else v
    return 0.0


def _pd(text: str) -> str:
    """Parse date: 'Jul 23, 2025' -> '2025-07-23'"""
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return text.strip()


# ─── Phone-to-Name mapping ──────────────────────────────────────────

PHONE_NAMES = {
    "(501) 413-4884": "Sathish",
    "(469) 328-8386": "Ganesh",
    "(469) 859-9780": "Ramya",
    "(501) 413-4892": "Sathya",
    "(608) 622-3521": "Parthiban",
    "(608) 609-3416": "Dharshana",
    "(608) 770-2157": "Milan Kumar",
    "(224) 770-0757": "Prince",
    "(608) 207-8787": "Deepika",
    "(224) 572-1750": "Sujitha",
    "(501) 249-7415": "Mani",
    "(331) 707-6438": "Meena",
}


# ─── DDL ────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS invoices (
    file_name              VARCHAR PRIMARY KEY,
    account_number         VARCHAR,
    bill_date              DATE,
    bill_month             VARCHAR,
    due_date               DATE,
    total_due              DECIMAL(10,2),
    plans_total            DECIMAL(10,2),
    equipment_total        DECIMAL(10,2),
    services_total         DECIMAL(10,2),
    onetime_charges        DECIMAL(10,2),
    total_savings          DECIMAL(10,2),
    previous_balance       DECIMAL(10,2),
    voice_line_count       INTEGER,
    connected_device_count INTEGER,
    wearable_count         INTEGER
);
CREATE TABLE IF NOT EXISTS line_charges (
    file_name         VARCHAR,
    bill_date         DATE,
    phone_number      VARCHAR,
    line_type         VARCHAR,
    status            VARCHAR,
    annotation        VARCHAR,
    plans_charge      DECIMAL(10,2),
    equipment_charge  DECIMAL(10,2),
    services_charge   DECIMAL(10,2),
    onetime_charge    DECIMAL(10,2),
    line_total        DECIMAL(10,2),
    PRIMARY KEY (file_name, phone_number)
);
CREATE TABLE IF NOT EXISTS lines (
    phone_number      VARCHAR PRIMARY KEY,
    line_type         VARCHAR,
    first_seen_date   DATE,
    last_seen_date    DATE,
    months_active     INTEGER,
    is_current        BOOLEAN,
    lifecycle_notes   VARCHAR
);
CREATE TABLE IF NOT EXISTS persons (
    person_id    INTEGER PRIMARY KEY,
    person_label VARCHAR
);
CREATE TABLE IF NOT EXISTS person_lines (
    person_id       INTEGER,
    phone_number    VARCHAR,
    effective_from  DATE,
    effective_to    DATE,
    relationship    VARCHAR,
    PRIMARY KEY (person_id, phone_number, effective_from)
);
CREATE TABLE IF NOT EXISTS savings (
    file_name      VARCHAR,
    bill_date      DATE,
    discount_type  VARCHAR,
    amount         DECIMAL(10,2),
    PRIMARY KEY (file_name, discount_type)
);
CREATE TABLE IF NOT EXISTS payments (
    file_name    VARCHAR,
    bill_date    DATE,
    entry_type   VARCHAR,
    description  VARCHAR,
    amount       DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS phone_names (
    phone_number  VARCHAR PRIMARY KEY,
    person_name   VARCHAR NOT NULL
);
CREATE OR REPLACE VIEW person_monthly_cost AS
SELECT
    p.person_id, p.person_label,
    lc.bill_date, lc.phone_number, lc.line_type, lc.status,
    lc.plans_charge, lc.equipment_charge,
    lc.services_charge, lc.onetime_charge, lc.line_total
FROM line_charges lc
JOIN person_lines pl
  ON lc.phone_number = pl.phone_number
 AND lc.bill_date >= pl.effective_from
 AND (pl.effective_to IS NULL OR lc.bill_date <= pl.effective_to)
JOIN persons p ON p.person_id = pl.person_id;
CREATE OR REPLACE VIEW person_total AS
SELECT
    person_id, person_label,
    COUNT(DISTINCT bill_date) AS months,
    SUM(plans_charge)     AS total_plans,
    SUM(equipment_charge) AS total_equipment,
    SUM(services_charge)  AS total_services,
    SUM(onetime_charge)   AS total_onetime,
    SUM(line_total)       AS grand_total
FROM person_monthly_cost
GROUP BY person_id, person_label;
"""




# ─── helpers: detailed-charges plan extraction ─────────────────────

def _extract_real_plan_charges(pdf, start_page: int = 1) -> dict:
    """
    Scan the DETAILED CHARGES pages for VOICE LINES, CONNECTED DEVICES,
    and WEARABLE sections and return the *real* per-line plan charges.

    T-Mobile's summary table rolls taxes/fees into the 'Plans' column
    for ALL line types (Plans == Plan + Taxes, Services shows only add-ons).
    The detailed section lists the actual plan charge before taxes.

    Returns  {phone_or_account: plan_charge}  (accumulated for mid-cycle).
    """
    real_plan: dict[str, float] = {}
    section: str | None = None          # "voice", "mi", or "wearable"

    _SECTION_RESET = re.compile(
        r"TAXES|EQUIPMENT|HANDSETS|YOUR\sLAST\sBILL|"
        r"THIS\sBILL\sSUMMARY|REGULAR\s+CHARGES|"
        r"MID-CYCLE|SERVICES", re.IGNORECASE)
    _PHONE_CHARGE = re.compile(
        r"(\(\d{3}\)\s*\d{3}-\d{4})\s+.+?\s+\$(\d+\.\d{2})")
    _PHONE_INCLUDED = re.compile(
        r"(\(\d{3}\)\s*\d{3}-\d{4})\s+.+?Included")
    _ACCOUNT_CHARGE = re.compile(
        r"Account\s+.+?\$(\d+\.\d{2})")

    # Scan only the first 6 pages after start_page (detailed charges appear early)
    end_page = min(start_page + 6, len(pdf.pages))
    for pg in pdf.pages[start_page:end_page]:
        text = pg.extract_text() or ""
        for line in text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue

            # Detect section boundaries
            if _SECTION_RESET.search(stripped):
                section = None
            # Section starts (order matters: check after reset)
            if "VOICE LINE" in stripped.upper() or "VOICE LINES" in stripped.upper():
                section = "voice"
                continue
            if "CONNECTED" in stripped and "DEVICE" in stripped:
                section = "mi"
                continue
            if re.search(r"(?<!\d\s)WEARABLE", stripped) and "$" not in stripped.split("WEARABLE")[0]:
                section = "wearable"
                continue

            if section not in ("voice", "mi", "wearable"):
                continue

            # Account plan charge (only in voice section)
            if section == "voice":
                am = _ACCOUNT_CHARGE.match(stripped)
                if am:
                    real_plan["Account"] = float(am.group(1))
                    continue
                # "Included" lines: plan is $0 (covered by account charge)
                im = _PHONE_INCLUDED.match(stripped)
                if im:
                    real_plan[im.group(1)] = 0.0
                    continue

            # Phone with explicit charge
            m = _PHONE_CHARGE.match(stripped)
            if m:
                phone = m.group(1)
                charge = float(m.group(2))
                real_plan[phone] = real_plan.get(phone, 0.0) + charge

    return real_plan


# ─── PDF extraction ────────────────────────────────────────────────

def extract_bill(source, file_name: str | None = None) -> dict:
    """
    Parse a T-Mobile bill PDF.

    Args:
        source: file path (str) or file-like object (BytesIO from upload)
        file_name: override filename (used when source is a BytesIO)

    Returns:
        dict with keys: invoice, lines, savings, payments
    """
    if isinstance(source, str):
        import os
        fname = file_name or os.path.basename(source)
        pdf = pdfplumber.open(source)
    else:
        fname = file_name or "uploaded.pdf"
        pdf = pdfplumber.open(source)

    p1 = pdf.pages[0].extract_text() or ""
    p2 = pdf.pages[1].extract_text() or "" if len(pdf.pages) > 1 else ""

    # ── invoice header (page 1) ──────────────────────────────────────
    def s1(pat, default=""):
        m = re.search(pat, p1)
        return m.group(1) if m else default

    bill_date = _pd(s1(r"Bill issue date\s+Account\s+Page\s*\n\s*(\w+ \d{1,2}, \d{4})"))
    account   = s1(r"(\d{9,})")
    total_due = float(s1(r"TOTAL DUE\s*\n?\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    due_date  = _pd(s1(r"due by (\w+ \d{1,2}, \d{4})", bill_date))
    _bill_month_raw = s1(r"Here.s your bill for (\w+)", "")
    try:
        bill_month = datetime.strptime(bill_date, "%Y-%m-%d").strftime("%b %Y")
    except ValueError:
        bill_month = _bill_month_raw

    plans_total = float(s1(r"PLANS.*?=\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    eq_raw = s1(r"EQUIPMENT.*?=\s*(-?\$?[\d,]+\.\d{2})", "0")
    equipment_total = _pm(eq_raw)
    services_total  = float(s1(r"SERVICES\s*\n\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    onetime_total   = float(s1(r"ONE-TIME CHARGES\s*\n\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    total_savings   = float(s1(r"YOU HAVE\s*\n?\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))

    voice_lines = int(s1(r"(\d+)\s+VOICE\s+LINE", "0"))
    connected_devices = int(s1(r"(\d+)\s+CONNECTED\s+DEVICE", "0"))
    wearables = int(s1(r"(\d+)\s+WEARABLE", "0"))

    prev_balance = 0.0
    m = re.search(r"Previous balance\s+\$?([\d,]+\.\d{2})", p2)
    if m:
        prev_balance = float(m.group(1).replace(",", ""))

    invoice = dict(
        file_name=fname, account_number=account, bill_date=bill_date,
        bill_month=bill_month, due_date=due_date, total_due=total_due,
        plans_total=plans_total, equipment_total=equipment_total,
        services_total=services_total, onetime_charges=onetime_total,
        total_savings=total_savings, previous_balance=prev_balance,
        voice_line_count=voice_lines, connected_device_count=connected_devices,
        wearable_count=wearables,
    )

    # ── line charges ─────────────────────────────────────────────────
    has_onetime_col = bool(re.search(r"One-time charges", p2))
    line_list = []

    summary_match = re.search(
        r"THIS BILL SUMMARY\s*\n.*?\n(.*?)(?=DETAILED CHARGES|YOU SAVED|$)",
        p2, re.DOTALL)
    block = summary_match.group(1) if summary_match else p2

    for raw_line in block.split("\n"):
        raw_line = raw_line.strip()
        if not raw_line or raw_line.startswith("Totals") or raw_line.startswith("Line "):
            continue

        m = re.match(
            r"(\(\d{3}\)\s*\d{3}-\d{4})"
            r"((?:\s*-\s*[A-Za-z][\w\s\-]*?)?)"
            r"\s+(Voice|Mobile Internet|Wearable)"
            r"\s+(.+)$",
            raw_line
        )
        if not m:
            m_acct = re.match(r"Account\s+(.+)$", raw_line)
            if m_acct:
                charges = m_acct.group(1).split()
                if has_onetime_col and len(charges) >= 5:
                    plan_c, eq_c, svc_c, ot_c, tot_c = charges[0], charges[1], charges[2], charges[3], charges[4]
                elif len(charges) >= 4:
                    plan_c, eq_c, svc_c, tot_c = charges[0], charges[1], charges[2], charges[3]
                    ot_c = "-"
                else:
                    continue
                line_list.append(dict(
                    file_name=fname, bill_date=bill_date, phone_number="Account",
                    line_type="Account", status="active", annotation="",
                    plans_charge=_pm(plan_c), equipment_charge=_pm(eq_c),
                    services_charge=_pm(svc_c), onetime_charge=_pm(ot_c),
                    line_total=_pm(tot_c),
                ))
            continue

        phone = m.group(1)
        annotation = m.group(2).strip().strip("-").strip()
        line_type = m.group(3)
        rest = m.group(4).strip()

        status = "active"
        ann_lower = annotation.lower()
        if "removed" in ann_lower:
            status = "removed"
        elif "old number" in ann_lower:
            status = "cancelled"
        elif "transferred" in ann_lower:
            status = "transferred_in"
        elif "new" in ann_lower:
            status = "new"

        charges = rest.split()
        if has_onetime_col:
            if len(charges) >= 5:
                plan_c, eq_c, svc_c, ot_c, tot_c = charges[:5]
            elif len(charges) == 4:
                plan_c, eq_c, svc_c, tot_c = charges[:4]; ot_c = "-"
            else:
                continue
        else:
            if len(charges) >= 4:
                plan_c, eq_c, svc_c, tot_c = charges[:4]; ot_c = "-"
            elif len(charges) == 3:
                plan_c, eq_c, tot_c = charges[:3]; svc_c = "-"; ot_c = "-"
            else:
                continue

        line_list.append(dict(
            file_name=fname, bill_date=bill_date, phone_number=phone,
            line_type=line_type, status=status, annotation=annotation,
            plans_charge=_pm(plan_c), equipment_charge=_pm(eq_c),
            services_charge=_pm(svc_c), onetime_charge=_pm(ot_c),
            line_total=_pm(tot_c),
        ))

    # ── fix plan charges for ALL line types ────────────────────────
    # T-Mobile's summary table bundles taxes/fees into the "Plans"
    # column for all line types.  Extract the real plan charges from
    # the DETAILED CHARGES section so we can compute:
    #   taxes = line_total - plans_charge - equipment - services - onetime
    real_plan = _extract_real_plan_charges(pdf, start_page=1)
    for entry in line_list:
        phone = entry["phone_number"]
        if phone in real_plan:
            old_plans = entry["plans_charge"]
            new_plans = round(real_plan[phone], 2)
            if new_plans < old_plans:
                entry["plans_charge"] = new_plans

    # ── savings ──────────────────────────────────────────────────────
    sav = []
    for label, pat in [
        ("AutoPay discounts",  r"AutoPay discounts\s+\$?([\d,]+\.\d{2})"),
        ("Service discounts",  r"Service discounts\s+\$?([\d,]+\.\d{2})"),
        ("Device discounts",   r"Device discounts\s+\$?([\d,]+\.\d{2})"),
    ]:
        m = re.search(pat, p2)
        if m:
            sav.append(dict(
                file_name=fname, bill_date=bill_date,
                discount_type=label,
                amount=float(m.group(1).replace(",", "")),
            ))

    # ── payments & credits ───────────────────────────────────────────
    pay = []
    for m in re.finditer(r"Payment\s*-\s*thank you\s+(\w+ \d{1,2})\s+(-?\$?[\d,]+\.\d{2})", p2):
        pay.append(dict(
            file_name=fname, bill_date=bill_date,
            entry_type="payment", description=f"Payment {m.group(1)}",
            amount=_pm(m.group(2)),
        ))
    for m in re.finditer(r"Credits\s*&\s*adjustments\s+(.*?)\s+(\w+ \d{1,2})\s+(-?\$?[\d,]+\.\d{2})", p2):
        pay.append(dict(
            file_name=fname, bill_date=bill_date,
            entry_type="credit", description=m.group(1).strip(),
            amount=_pm(m.group(3)),
        ))

    pdf.close()

    return dict(
        invoice=invoice, lines=line_list, savings=sav, payments=pay,
    )


# ─── DB operations ──────────────────────────────────────────────────

def init_schema(con: duckdb.DuckDBPyConnection):
    """Create all tables and views if they don't exist."""
    for stmt in DDL.split(";"):
        s = stmt.strip()
        if s:
            con.execute(s)


def load_bill_data(con: duckdb.DuckDBPyConnection, data: dict):
    """Insert parsed bill data into the DB."""
    inv = data["invoice"]
    con.execute("INSERT OR REPLACE INTO invoices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [inv["file_name"], inv["account_number"], inv["bill_date"],
         inv["bill_month"], inv["due_date"], inv["total_due"],
         inv["plans_total"], inv["equipment_total"], inv["services_total"],
         inv["onetime_charges"], inv["total_savings"], inv["previous_balance"],
         inv["voice_line_count"], inv["connected_device_count"],
         inv["wearable_count"]])

    for r in data["lines"]:
        con.execute("INSERT OR REPLACE INTO line_charges VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [r["file_name"], r["bill_date"], r["phone_number"],
             r["line_type"], r["status"], r["annotation"],
             r["plans_charge"], r["equipment_charge"],
             r["services_charge"], r["onetime_charge"], r["line_total"]])

    for r in data["savings"]:
        con.execute("INSERT OR REPLACE INTO savings VALUES (?,?,?,?)",
            [r["file_name"], r["bill_date"], r["discount_type"], r["amount"]])

    for r in data["payments"]:
        con.execute("INSERT INTO payments VALUES (?,?,?,?,?)",
            [r["file_name"], r["bill_date"], r["entry_type"],
             r["description"], r["amount"]])


def rebuild_dimensions(con: duckdb.DuckDBPyConnection):
    """Rebuild lines + persons dimensions from fact table."""
    # Lines dimension
    con.execute("DELETE FROM lines")
    con.execute("""
        INSERT INTO lines
        SELECT
            phone_number,
            LAST(line_type ORDER BY bill_date)   AS line_type,
            MIN(bill_date)                       AS first_seen_date,
            MAX(bill_date)                       AS last_seen_date,
            COUNT(DISTINCT bill_date)            AS months_active,
            MAX(bill_date) = (SELECT MAX(bill_date) FROM invoices) AS is_current,
            CASE
                WHEN MAX(CASE WHEN status='removed'   THEN 1 ELSE 0 END)=1
                    THEN 'Removed '||strftime(MAX(CASE WHEN status='removed' THEN bill_date END),'%b %Y')
                WHEN MAX(CASE WHEN status='cancelled'  THEN 1 ELSE 0 END)=1
                    THEN 'Cancelled '||strftime(MAX(CASE WHEN status='cancelled' THEN bill_date END),'%b %Y')
                WHEN MIN(CASE WHEN status='new' THEN bill_date END) IS NOT NULL
                    THEN 'Added '||strftime(MIN(CASE WHEN status='new' THEN bill_date END),'%b %Y')
                WHEN MIN(CASE WHEN status='transferred_in' THEN bill_date END) IS NOT NULL
                    THEN 'Transferred '||strftime(MIN(CASE WHEN status='transferred_in' THEN bill_date END),'%b %Y')
                ELSE 'Original'
            END AS lifecycle_notes
        FROM line_charges
        WHERE phone_number != 'Account'
        GROUP BY phone_number
    """)

    # Seed phone_names from PHONE_NAMES dict (only for phones not already in the table)
    for phone, name in PHONE_NAMES.items():
        con.execute(
            "INSERT OR IGNORE INTO phone_names VALUES (?,?)", [phone, name]
        )

    # Build name lookup: DB overrides take priority, fallback to hardcoded
    db_names = {}
    for row in con.execute("SELECT phone_number, person_name FROM phone_names").fetchall():
        db_names[row[0]] = row[1]

    # Person mapping
    con.execute("DELETE FROM persons")
    con.execute("DELETE FROM person_lines")

    rows = con.execute("""
        SELECT phone_number, line_type, first_seen_date, last_seen_date
        FROM lines ORDER BY first_seen_date, phone_number
    """).fetchall()

    pid = 0
    for phone, ltype, first_dt, last_dt in rows:
        pid += 1
        rel = "voice" if ltype == "Voice" else ("wearable" if ltype == "Wearable" else "connected_device")
        name = db_names.get(phone, PHONE_NAMES.get(phone, phone))
        con.execute("INSERT INTO persons VALUES (?,?)", [pid, name])
        con.execute("INSERT INTO person_lines VALUES (?,?,?,?,?)",
                    [pid, phone, str(first_dt), None, rel])
