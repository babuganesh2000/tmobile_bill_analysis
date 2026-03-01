"""
Comprehensive T-Mobile PDF bill parser → DuckDB.

Tables:
  invoices       – header-level totals per bill (14 rows for 14 months)
  line_charges   – per-phone-number charges per bill (core fact table)
  lines          – dimension: every phone number ever seen, with lifecycle info
  persons        – dimension: Person 1..N (editable mapping)
  person_lines   – bridge: phone_number → person (supports history)
  savings        – discount breakdown per bill
  payments       – payments & credits per bill

Views:
  person_monthly_cost  – joins line_charges → person_lines → persons
  person_total         – aggregated per-person across all months
"""

import os, re, duckdb, pdfplumber
from datetime import datetime

PDF_DIR = r"c:\tmobile"
DB_PATH = os.path.join(PDF_DIR, "tmobile_bills.duckdb")

# ─── helpers ────────────────────────────────────────────────────────

def pm(text: str) -> float:
    """Parse money: '$267.57' → 267.57; '-$38.00' → -38.0; '-' → 0.0"""
    if not text or text.strip() in ("-", ""):
        return 0.0
    t = text.strip().replace(",", "")
    neg = t.startswith("-")
    m = re.search(r"\$?([\d]+\.?\d*)", t)
    if m:
        v = float(m.group(1))
        return -v if neg else v
    return 0.0

def pd(text: str) -> str:
    """Parse date: 'Jul 23, 2025' → '2025-07-23'"""
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return text.strip()

# ─── extraction ─────────────────────────────────────────────────────

def extract_bill(pdf_path: str) -> dict:
    fname = os.path.basename(pdf_path)
    pdf = pdfplumber.open(pdf_path)
    p1 = pdf.pages[0].extract_text() or ""
    p2 = pdf.pages[1].extract_text() or "" if len(pdf.pages) > 1 else ""

    # ── invoice header (page 1) ──────────────────────────────────────
    def s1(pat, default=""):
        m = re.search(pat, p1)
        return m.group(1) if m else default

    bill_date = pd(s1(r"Bill issue date\s+Account\s+Page\s*\n\s*(\w+ \d{1,2}, \d{4})"))
    account   = s1(r"(\d{9,})")
    total_due = float(s1(r"TOTAL DUE\s*\n?\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    due_date  = pd(s1(r"due by (\w+ \d{1,2}, \d{4})", bill_date))
    bill_month = s1(r"Here.s your bill for (\w+)", "")

    plans_total = float(s1(r"PLANS.*?=\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    # Equipment can be negative
    eq_raw = s1(r"EQUIPMENT.*?=\s*(-?\$?[\d,]+\.\d{2})", "0")
    equipment_total = pm(eq_raw)
    services_total  = float(s1(r"SERVICES\s*\n\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    onetime_total   = float(s1(r"ONE-TIME CHARGES\s*\n\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))
    total_savings   = float(s1(r"YOU HAVE\s*\n?\s*\$?([\d,]+\.\d{2})", "0").replace(",", ""))

    voice_lines = int(s1(r"(\d+)\s+VOICE\s+LINE", "0"))
    connected_devices = int(s1(r"(\d+)\s+CONNECTED\s+DEVICE", "0"))
    wearables = int(s1(r"(\d+)\s+WEARABLE", "0"))

    # Previous balance from page 2
    prev_balance = 0.0
    m = re.search(r"Previous balance\s+\$?([\d,]+\.\d{2})", p2)
    if m:
        prev_balance = float(m.group(1).replace(",", ""))

    # Data usage
    data_gb = float(s1(r"([\d,]+\.?\d*)\s*GB\s+of\s+data", "0").replace(",", ""))

    invoice = dict(
        file_name=fname, account_number=account, bill_date=bill_date,
        bill_month=bill_month, due_date=due_date, total_due=total_due,
        plans_total=plans_total, equipment_total=equipment_total,
        services_total=services_total, onetime_charges=onetime_total,
        total_savings=total_savings, previous_balance=prev_balance,
        voice_line_count=voice_lines, connected_device_count=connected_devices,
        wearable_count=wearables, data_used_gb=data_gb,
    )

    # ── Detect column format: 4-column vs 5-column ──────────────────
    has_onetime_col = bool(re.search(r"One-time charges", p2))

    # ── line charges from THIS BILL SUMMARY (page 2) ────────────────
    lines = []

    # Find the summary block between "THIS BILL SUMMARY" and "DETAILED CHARGES"
    summary_match = re.search(r"THIS BILL SUMMARY\s*\n.*?\n(.*?)(?=DETAILED CHARGES|YOU SAVED|$)",
                              p2, re.DOTALL)
    if summary_match:
        block = summary_match.group(1)
    else:
        block = p2

    # Parse each line in the block
    for raw_line in block.split("\n"):
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        # Match phone number (with optional annotation) or "Account"
        # Formats observed:
        #   (469) 328-8386 Voice $5.15 - - $5.15
        #   (608) 805-3969 - New Voice $20.99 $29.17 $17.50 - $67.66
        #   (608) 805-3969 - Old number Voice -$10.44 -$26.25 -$14.50 - -$51.19
        #   (501) 413-4884 - Removed Voice $2.90 - - $2.90
        #   (501) 413-4884 - Transferred to T-Mobile Voice $28.00 ...
        #   (608) 980-2499 - New Wearable $24.29 $0.00 - $24.29
        #   Account $81.18 - - $81.18
        #   Totals $273.99 -$7.30 $0.00 $266.69

        # Skip header/totals
        if raw_line.startswith("Totals") or raw_line.startswith("Line "):
            continue

        # Try matching phone number lines
        m = re.match(
            r"(\(\d{3}\)\s*\d{3}-\d{4})"         # phone
            r"((?:\s*-\s*[A-Za-z][\w\s\-]*?)?)"   # optional annotation (allows hyphens for "T-Mobile")
            r"\s+(Voice|Mobile Internet|Wearable)"  # line type
            r"\s+(.+)$",                            # rest (charges)
            raw_line
        )
        if not m:
            # Try Account line
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
                lines.append(dict(
                    file_name=fname, bill_date=bill_date, phone_number="Account",
                    line_type="Account", status="active", annotation="",
                    plans_charge=pm(plan_c), equipment_charge=pm(eq_c),
                    services_charge=pm(svc_c), onetime_charge=pm(ot_c),
                    line_total=pm(tot_c),
                ))
            continue

        phone = m.group(1)
        annotation = m.group(2).strip().strip("-").strip()
        line_type = m.group(3)
        rest = m.group(4).strip()

        # Determine status from annotation
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

        # Split charges
        charges = rest.split()
        if has_onetime_col:
            # 5-column: plans, equipment, services, one-time, total
            if len(charges) >= 5:
                plan_c, eq_c, svc_c, ot_c, tot_c = charges[0], charges[1], charges[2], charges[3], charges[4]
            elif len(charges) == 4:
                plan_c, eq_c, svc_c, tot_c = charges[0], charges[1], charges[2], charges[3]
                ot_c = "-"
            else:
                continue
        else:
            # 4-column: plans, equipment, services, total
            if len(charges) >= 4:
                plan_c, eq_c, svc_c, tot_c = charges[0], charges[1], charges[2], charges[3]
                ot_c = "-"
            elif len(charges) == 3:
                plan_c, eq_c, tot_c = charges[0], charges[1], charges[2]
                svc_c = "-"
                ot_c = "-"
            else:
                continue

        lines.append(dict(
            file_name=fname, bill_date=bill_date, phone_number=phone,
            line_type=line_type, status=status, annotation=annotation,
            plans_charge=pm(plan_c), equipment_charge=pm(eq_c),
            services_charge=pm(svc_c), onetime_charge=pm(ot_c),
            line_total=pm(tot_c),
        ))

    # ── savings ──────────────────────────────────────────────────────
    savings = []
    for label, pat in [
        ("AutoPay discounts",  r"AutoPay discounts\s+\$?([\d,]+\.\d{2})"),
        ("Service discounts",  r"Service discounts\s+\$?([\d,]+\.\d{2})"),
        ("Device discounts",   r"Device discounts\s+\$?([\d,]+\.\d{2})"),
    ]:
        m = re.search(pat, p2)
        if m:
            savings.append(dict(
                file_name=fname, bill_date=bill_date,
                discount_type=label,
                amount=float(m.group(1).replace(",", "")),
            ))

    # ── payments & credits ───────────────────────────────────────────
    payments = []
    for m in re.finditer(r"Payment\s*-\s*thank you\s+(\w+ \d{1,2})\s+(-?\$?[\d,]+\.\d{2})", p2):
        payments.append(dict(
            file_name=fname, bill_date=bill_date,
            entry_type="payment", description=f"Payment {m.group(1)}",
            amount=pm(m.group(2)),
        ))
    for m in re.finditer(r"Credits\s*&\s*adjustments\s+(.*?)\s+(\w+ \d{1,2})\s+(-?\$?[\d,]+\.\d{2})", p2):
        payments.append(dict(
            file_name=fname, bill_date=bill_date,
            entry_type="credit", description=m.group(1).strip(),
            amount=pm(m.group(3)),
        ))

    # ── usage details (requires full PDF still open) ──────────────────
    try:
        usage_data = extract_usage_details(pdf, bill_date, fname)
    except Exception as e:
        print(f"  Warning: usage extraction failed ({e}), skipping usage for {fname}")
        usage_data = {"line_usage": [], "account_usage": {"file_name": fname, "bill_date": bill_date, "total_talk_minutes": 0, "total_messages": 0, "total_data_gb": 0.0}}
    pdf.close()

    return dict(
        invoice=invoice, lines=lines, savings=savings, payments=payments,
        line_usage=usage_data["line_usage"],
        account_usage=usage_data["account_usage"],
    )


# ─── Usage detail extraction ───────────────────────────────────────

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

def _safe_extract_text(page, timeout=15):
    """Extract text from a PDF page with a timeout to handle pdfminer hangs."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(page.extract_text)
        try:
            return future.result(timeout=timeout) or ""
        except (FuturesTimeout, Exception):
            return ""

def extract_usage_details(pdf, bill_date: str, file_name: str) -> dict:
    """Extract per-line usage (talk minutes, data GB) from USAGE DETAILS pages."""
    # 1) Parse "YOU USED" summary from pages 2-3
    line_data_gb = {}
    total_talk = 0
    total_msgs = 0
    total_data_gb = 0.0

    for i in [1, 2]:
        if i >= len(pdf.pages):
            break
        text = _safe_extract_text(pdf.pages[i])
        for m in re.finditer(r"\((\d{3})\)\s*(\d{3}-\d{4})\s+([\d.]+)GB", text):
            phone = f"({m.group(1)}) {m.group(2)}"
            line_data_gb[phone] = float(m.group(3))
        for m in re.finditer(r"([\d,]+)\s+minutes\s+of\s+talk", text):
            total_talk = int(m.group(1).replace(",", ""))
        for m in re.finditer(r"([\d,]+)\s+messages", text):
            total_msgs = int(m.group(1).replace(",", ""))
        for m in re.finditer(r"([\d,.]+)\s*GB\s+of\s+data", text):
            total_data_gb = float(m.group(1).replace(",", ""))

    # 2) Parse USAGE DETAILS pages for per-line talk_minutes and data_mb
    line_usage_map = {}
    phone_sections = {}
    active_phone_by_section = {}

    for page_idx in range(4, len(pdf.pages)):
        text = _safe_extract_text(pdf.pages[page_idx])
        if not text:
            continue
        events = []

        for m in re.finditer(
            r"\((\d{3})\)\s*(\d{3}-\d{4})\s+\w+ \d+\s*-\s*\w+ \d+", text
        ):
            phone = f"({m.group(1)}) {m.group(2)}"
            events.append((m.start(), "header", phone, None))

        for m in re.finditer(
            r"CONTINUED\s*-\s*\((\d{3})\)\s*(\d{3}-\d{4})\s*,\s*(\w+)", text
        ):
            phone = f"({m.group(1)}) {m.group(2)}"
            section = m.group(3).upper()
            events.append((m.start(), "continued", phone, section))

        for m in re.finditer(r"Totals\s+([\d,]+\.?\d*)\s+\$[\d,.]+", text):
            val_str = m.group(1).replace(",", "")
            events.append((m.start(), "totals_num", None, val_str))

        events.sort(key=lambda x: x[0])

        for _pos, etype, phone, val in events:
            if etype == "header":
                if phone not in line_usage_map:
                    line_usage_map[phone] = {"talk_minutes": 0, "data_mb": 0.0}
                if phone not in phone_sections:
                    phone_sections[phone] = set()
                active_phone_by_section["TALK"] = phone
            elif etype == "continued":
                if phone not in line_usage_map:
                    line_usage_map[phone] = {"talk_minutes": 0, "data_mb": 0.0}
                if phone not in phone_sections:
                    phone_sections[phone] = set()
                active_phone_by_section[val] = phone
            elif etype == "totals_num":
                val_f = float(val)
                if "." in val and len(val.split(".")[1]) > 2:
                    target = active_phone_by_section.get("DATA")
                    if not target:
                        for s in ["DATA", "TEXT", "TALK"]:
                            if s in active_phone_by_section:
                                target = active_phone_by_section[s]
                                break
                    if target and "data" not in phone_sections.get(target, set()):
                        line_usage_map[target]["data_mb"] = val_f
                        phone_sections.setdefault(target, set()).add("data")
                else:
                    target = active_phone_by_section.get("TALK")
                    if target and "talk" not in phone_sections.get(target, set()):
                        line_usage_map[target]["talk_minutes"] = int(val_f)
                        phone_sections.setdefault(target, set()).add("talk")

    # Merge
    all_phones = set(line_data_gb.keys()) | set(line_usage_map.keys())
    line_usage_list = []
    for phone in sorted(all_phones):
        talk_min = line_usage_map.get(phone, {}).get("talk_minutes", 0)
        data_mb = line_usage_map.get(phone, {}).get("data_mb", 0.0)
        data_gb = line_data_gb.get(phone, data_mb / 1024.0)
        if data_gb > 0 and data_mb == 0:
            data_mb = data_gb * 1024.0
        line_usage_list.append(dict(
            file_name=file_name, bill_date=bill_date, phone_number=phone,
            talk_minutes=talk_min, data_mb=round(data_mb, 4),
            data_gb=data_gb,
        ))

    account_usage = dict(
        file_name=file_name, bill_date=bill_date,
        total_talk_minutes=total_talk, total_messages=total_msgs,
        total_data_gb=total_data_gb,
    )

    return dict(line_usage=line_usage_list, account_usage=account_usage)


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
    wearable_count         INTEGER,
    data_used_gb           DECIMAL(10,2)
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

CREATE TABLE IF NOT EXISTS line_usage (
    file_name        VARCHAR,
    bill_date        DATE,
    phone_number     VARCHAR,
    talk_minutes     INTEGER,
    data_mb          DECIMAL(12,4),
    data_gb          DECIMAL(10,2),
    PRIMARY KEY (file_name, phone_number)
);

CREATE TABLE IF NOT EXISTS account_usage (
    file_name            VARCHAR PRIMARY KEY,
    bill_date            DATE,
    total_talk_minutes   INTEGER,
    total_messages       INTEGER,
    total_data_gb        DECIMAL(10,2)
);

CREATE OR REPLACE VIEW person_monthly_cost AS
SELECT
    p.person_id,
    p.person_label,
    lc.bill_date,
    lc.phone_number,
    lc.line_type,
    lc.status,
    lc.plans_charge,
    lc.equipment_charge,
    lc.services_charge,
    lc.onetime_charge,
    lc.line_total
FROM line_charges lc
JOIN person_lines pl
  ON lc.phone_number = pl.phone_number
 AND lc.bill_date >= pl.effective_from
 AND (pl.effective_to IS NULL OR lc.bill_date <= pl.effective_to)
JOIN persons p ON p.person_id = pl.person_id;

CREATE OR REPLACE VIEW person_total AS
SELECT
    person_id,
    person_label,
    COUNT(DISTINCT bill_date) AS months,
    SUM(plans_charge)     AS total_plans,
    SUM(equipment_charge) AS total_equipment,
    SUM(services_charge)  AS total_services,
    SUM(onetime_charge)   AS total_onetime,
    SUM(line_total)       AS grand_total
FROM person_monthly_cost
GROUP BY person_id, person_label;

CREATE OR REPLACE VIEW line_usage_monthly AS
SELECT
    lu.bill_date,
    i.bill_month,
    lu.phone_number,
    l.line_type,
    lu.talk_minutes,
    lu.data_mb,
    lu.data_gb,
    lc.line_total
FROM line_usage lu
JOIN invoices i ON i.file_name = lu.file_name
LEFT JOIN lines l ON l.phone_number = lu.phone_number
LEFT JOIN line_charges lc ON lc.file_name = lu.file_name AND lc.phone_number = lu.phone_number;

CREATE OR REPLACE VIEW usage_summary AS
SELECT
    au.bill_date,
    i.bill_month,
    au.total_talk_minutes,
    au.total_messages,
    au.total_data_gb,
    i.total_due,
    i.voice_line_count,
    i.connected_device_count,
    i.wearable_count
FROM account_usage au
JOIN invoices i ON i.file_name = au.file_name;
"""

# ─── load helpers ───────────────────────────────────────────────────

def load_invoice(con, inv):
    con.execute("INSERT OR REPLACE INTO invoices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [inv["file_name"], inv["account_number"], inv["bill_date"],
         inv["bill_month"], inv["due_date"], inv["total_due"],
         inv["plans_total"], inv["equipment_total"], inv["services_total"],
         inv["onetime_charges"], inv["total_savings"], inv["previous_balance"],
         inv["voice_line_count"], inv["connected_device_count"],
         inv["wearable_count"], inv["data_used_gb"]])

def load_line_charges(con, recs):
    for r in recs:
        con.execute("INSERT OR REPLACE INTO line_charges VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [r["file_name"], r["bill_date"], r["phone_number"],
             r["line_type"], r["status"], r["annotation"],
             r["plans_charge"], r["equipment_charge"],
             r["services_charge"], r["onetime_charge"], r["line_total"]])

def load_savings(con, recs):
    for r in recs:
        con.execute("INSERT OR REPLACE INTO savings VALUES (?,?,?,?)",
            [r["file_name"], r["bill_date"], r["discount_type"], r["amount"]])

def load_payments(con, recs):
    for r in recs:
        con.execute("INSERT INTO payments VALUES (?,?,?,?,?)",
            [r["file_name"], r["bill_date"], r["entry_type"],
             r["description"], r["amount"]])

def load_line_usage(con, recs):
    for r in recs:
        con.execute("INSERT OR REPLACE INTO line_usage VALUES (?,?,?,?,?,?)",
            [r["file_name"], r["bill_date"], r["phone_number"],
             r["talk_minutes"], r["data_mb"], r["data_gb"]])

def load_account_usage(con, au):
    con.execute("INSERT OR REPLACE INTO account_usage VALUES (?,?,?,?,?)",
        [au["file_name"], au["bill_date"], au["total_talk_minutes"],
         au["total_messages"], au["total_data_gb"]])


def build_lines_dimension(con):
    """Build the lines dimension table from line_charges."""
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
                WHEN MAX(CASE WHEN status = 'removed'   THEN 1 ELSE 0 END) = 1
                    THEN 'Removed ' || strftime(MAX(CASE WHEN status='removed' THEN bill_date END), '%b %Y')
                WHEN MAX(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) = 1
                    THEN 'Cancelled ' || strftime(MAX(CASE WHEN status='cancelled' THEN bill_date END), '%b %Y')
                WHEN MIN(CASE WHEN status = 'new' THEN bill_date END) IS NOT NULL
                    THEN 'Added ' || strftime(MIN(CASE WHEN status='new' THEN bill_date END), '%b %Y')
                WHEN MIN(CASE WHEN status = 'transferred_in' THEN bill_date END) IS NOT NULL
                    THEN 'Transferred ' || strftime(MIN(CASE WHEN status='transferred_in' THEN bill_date END), '%b %Y')
                ELSE 'Original'
            END AS lifecycle_notes
        FROM line_charges
        WHERE phone_number != 'Account'
        GROUP BY phone_number
    """)


def build_person_mapping(con):
    """Auto-generate Person 1..N. Each unique phone gets a person_id."""
    con.execute("DELETE FROM persons")
    con.execute("DELETE FROM person_lines")

    rows = con.execute("""
        SELECT phone_number, line_type, first_seen_date, last_seen_date
        FROM lines ORDER BY first_seen_date, phone_number
    """).fetchall()

    pid = 0
    for phone, ltype, first_dt, last_dt in rows:
        pid += 1
        # Determine relationship label
        if ltype == "Voice":
            rel = "voice"
        elif ltype == "Wearable":
            rel = "wearable"
        else:
            rel = "connected_device"

        con.execute("INSERT INTO persons VALUES (?,?)", [pid, f"Person {pid}"])
        con.execute("INSERT INTO person_lines VALUES (?,?,?,?,?)",
                    [pid, phone, str(first_dt), None, rel])


# ─── main ───────────────────────────────────────────────────────────

def main():
    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])
    print(f"Found {len(pdfs)} PDF bills\n")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)

    for stmt in DDL.split(";"):
        s = stmt.strip()
        if s:
            con.execute(s)

    for fname in pdfs:
        path = os.path.join(PDF_DIR, fname)
        print(f"Processing: {fname}")
        data = extract_bill(path)
        inv = data["invoice"]
        print(f"  {inv['bill_date']}  {inv['bill_month']:<12} Total=${inv['total_due']:>8.2f}  "
              f"Lines:{len(data['lines']):>3}  Savings:{len(data['savings'])}  Payments:{len(data['payments'])}")
        load_invoice(con, inv)
        load_line_charges(con, data["lines"])
        load_savings(con, data["savings"])
        load_payments(con, data["payments"])
        load_line_usage(con, data["line_usage"])
        load_account_usage(con, data["account_usage"])
        au = data["account_usage"]
        print(f"  Usage: {au['total_talk_minutes']} min talk, {au['total_messages']} msgs, "
              f"{au['total_data_gb']} GB data, {len(data['line_usage'])} lines")

    print("\nBuilding dimensions...")
    build_lines_dimension(con)
    build_person_mapping(con)

    # ── Reports ──────────────────────────────────────────────────────
    print("\n" + "=" * 90)
    print("LOAD COMPLETE")
    print("=" * 90)

    print("\n─── INVOICES ───")
    rows = con.execute("""
        SELECT bill_date, bill_month, total_due, plans_total, equipment_total,
               services_total, onetime_charges, total_savings,
               voice_line_count, connected_device_count, wearable_count
        FROM invoices ORDER BY bill_date
    """).fetchall()
    hdr = f"{'Date':<12}{'Month':<10}{'Total':>9}{'Plans':>9}{'Equip':>8}{'Svc':>7}{'1-Time':>8}{'Save':>8}{'V':>3}{'D':>3}{'W':>3}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{str(r[0]):<12}{r[1]:<10}{r[2]:>9.2f}{r[3]:>9.2f}{r[4]:>8.2f}{r[5]:>7.2f}{r[6]:>8.2f}{r[7]:>8.2f}{r[8]:>3}{r[9]:>3}{r[10]:>3}")

    print("\n─── LINES DIMENSION ───")
    rows = con.execute("""
        SELECT phone_number, line_type, first_seen_date, last_seen_date,
               months_active, is_current, lifecycle_notes
        FROM lines ORDER BY line_type, first_seen_date, phone_number
    """).fetchall()
    hdr = f"{'Phone':<18}{'Type':<18}{'First':<12}{'Last':<12}{'Mths':>5}{'Cur':>5}  {'Notes'}"
    print(hdr)
    print("-" * 90)
    for r in rows:
        print(f"{r[0]:<18}{r[1]:<18}{str(r[2]):<12}{str(r[3]):<12}{r[4]:>5}{'Y' if r[5] else 'N':>5}  {r[6]}")

    print("\n─── PERSONS ───")
    rows = con.execute("""
        SELECT p.person_id, p.person_label, pl.phone_number, pl.relationship,
               l.line_type, l.months_active, l.is_current
        FROM person_lines pl
        JOIN persons p ON p.person_id = pl.person_id
        JOIN lines l ON l.phone_number = pl.phone_number
        ORDER BY p.person_id
    """).fetchall()
    hdr = f"{'ID':>3}  {'Label':<12}{'Phone':<18}{'Relation':<18}{'Type':<18}{'Mths':>5}{'Active'}"
    print(hdr)
    print("-" * 80)
    for r in rows:
        print(f"{r[0]:>3}  {r[1]:<12}{r[2]:<18}{r[3]:<18}{r[4]:<18}{r[5]:>5}{'  Y' if r[6] else '  N'}")

    # Validate line_charges totals vs invoice totals
    print("\n─── VALIDATION: line_charges total vs invoice total ───")
    rows = con.execute("""
        SELECT i.bill_date, i.total_due,
               SUM(lc.line_total) AS sum_lines,
               i.total_due - SUM(lc.line_total) AS diff
        FROM invoices i
        JOIN line_charges lc ON lc.file_name = i.file_name
        GROUP BY i.bill_date, i.total_due
        ORDER BY i.bill_date
    """).fetchall()
    print(f"{'Date':<12}{'Invoice':>10}{'Sum Lines':>12}{'Diff':>10}")
    print("-" * 44)
    for r in rows:
        flag = " ✓" if abs(float(r[3])) < 0.02 else " ✗"
        print(f"{str(r[0]):<12}{float(r[1]):>10.2f}{float(r[2]):>12.2f}{float(r[3]):>10.2f}{flag}")

    print("\n─── ROW COUNTS ───")
    for tbl in ["invoices", "line_charges", "lines", "persons", "person_lines",
                 "savings", "payments", "line_usage", "account_usage"]:
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<18} {cnt:>5} rows")

    con.close()
    print(f"\nDatabase: {DB_PATH}")


if __name__ == "__main__":
    main()
