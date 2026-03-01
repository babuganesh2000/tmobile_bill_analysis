"""
Test script for the usage detail extraction logic.
Validates per-line talk/data parsing against known totals.
"""
import pdfplumber, re, os

PDF_DIR = r"c:\tmobile"


def _pm(text: str) -> float:
    if not text or text.strip() in ("-", ""):
        return 0.0
    t = text.strip().replace(",", "")
    neg = t.startswith("-")
    m = re.search(r"\$?([\d]+\.?\d*)", t)
    if m:
        v = float(m.group(1))
        return -v if neg else v
    return 0.0


def extract_usage(pdf) -> dict:
    """
    Extract per-line usage details from a T-Mobile bill PDF.

    Returns dict with:
      - line_usage: {phone: {talk_minutes, data_mb, data_gb}}
      - account_usage: {total_talk_minutes, total_messages, total_data_gb}
    """
    # 1) Parse "YOU USED" summary from pages 2-3 for per-line data_gb and account totals
    line_data_gb = {}
    total_talk = 0
    total_msgs = 0
    total_data_gb = 0.0

    for i in [1, 2]:
        if i >= len(pdf.pages):
            break
        text = pdf.pages[i].extract_text() or ""

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
    # Collect events (position-ordered) from all usage detail pages
    all_events = []

    for page_idx in range(4, len(pdf.pages)):
        text = pdf.pages[page_idx].extract_text() or ""
        events = []

        # Phone headers: "(469) 328-8386 Jan 24 - Feb 23"
        for m in re.finditer(
            r"\((\d{3})\)\s*(\d{3}-\d{4})\s+\w+ \d+\s*-\s*\w+ \d+", text
        ):
            phone = f"({m.group(1)}) {m.group(2)}"
            events.append((m.start(), "header", phone, None))

        # CONTINUED markers: "CONTINUED - (469) 328-8386 , TALK"
        for m in re.finditer(
            r"CONTINUED\s*-\s*\((\d{3})\)\s*(\d{3}-\d{4})\s*,\s*(\w+)", text
        ):
            phone = f"({m.group(1)}) {m.group(2)}"
            section = m.group(3).upper()
            events.append((m.start(), "continued", phone, section))

        # Totals with numeric value: "Totals 408 $0.00" or "Totals 3,622.5318 $0.00"
        for m in re.finditer(r"Totals\s+([\d,]+\.?\d*)\s+\$[\d,.]+", text):
            val_str = m.group(1).replace(",", "")
            events.append((m.start(), "totals_num", None, val_str))

        # Totals cost-only: "Totals $0.00" (text section)
        for m in re.finditer(r"(?<![,\d])Totals\s+\$([\d,.]+)", text):
            events.append((m.start(), "totals_cost", None, m.group(1)))

        # Sort by position in text
        events.sort(key=lambda x: x[0])

        for pos, etype, phone, val in events:
            all_events.append((page_idx, pos, etype, phone, val))

    # Process events to build per-line usage
    line_usage = {}
    # Track which phones have been seen and what sections completed
    phone_sections = {}  # phone -> set of completed section types ('talk', 'data')
    # Track the active phone whose section is being described
    # Due to 2-column layout, we may see events for finishing phone and new phone
    # Key insight: CONTINUED markers explicitly name the phone and section
    active_phone_by_section = {}  # section_type -> phone

    for page_idx, pos, etype, phone, val in all_events:
        if etype == "header":
            if phone not in line_usage:
                line_usage[phone] = {"talk_minutes": 0, "data_mb": 0.0}
            if phone not in phone_sections:
                phone_sections[phone] = set()
            # A new phone header means its TALK section starts
            active_phone_by_section["TALK"] = phone

        elif etype == "continued":
            if phone not in line_usage:
                line_usage[phone] = {"talk_minutes": 0, "data_mb": 0.0}
            if phone not in phone_sections:
                phone_sections[phone] = set()
            active_phone_by_section[val] = phone

        elif etype == "totals_num":
            val_f = float(val)
            # Determine if talk (integer) or data (has significant decimals)
            if "." in val and len(val.split(".")[1]) > 2:
                # Data MB (typically 4 decimal places)
                target_phone = active_phone_by_section.get("DATA")
                if not target_phone:
                    # Fallback: find the most recently referenced phone that needs data
                    for section in ["DATA", "TEXT", "TALK"]:
                        if section in active_phone_by_section:
                            target_phone = active_phone_by_section[section]
                            break
                if target_phone and "data" not in phone_sections.get(target_phone, set()):
                    line_usage[target_phone]["data_mb"] = val_f
                    phone_sections.setdefault(target_phone, set()).add("data")
            else:
                # Talk minutes (integer)
                target_phone = active_phone_by_section.get("TALK")
                if target_phone and "talk" not in phone_sections.get(target_phone, set()):
                    line_usage[target_phone]["talk_minutes"] = int(val_f)
                    phone_sections.setdefault(target_phone, set()).add("talk")

    # Merge data_gb from summary
    for phone, gb in line_data_gb.items():
        if phone not in line_usage:
            line_usage[phone] = {"talk_minutes": 0, "data_mb": gb * 1024}
        line_usage[phone]["data_gb"] = gb

    # Also set data_gb from parsed data_mb for phones not in summary
    for phone, usage in line_usage.items():
        if "data_gb" not in usage:
            usage["data_gb"] = usage["data_mb"] / 1024.0

    account_usage = {
        "total_talk_minutes": total_talk,
        "total_messages": total_msgs,
        "total_data_gb": total_data_gb,
    }

    return {"line_usage": line_usage, "account_usage": account_usage}


def main():
    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])

    for fname in pdfs[:3]:  # Test first 3 and last bill
        path = os.path.join(PDF_DIR, fname)
        pdf = pdfplumber.open(path)
        print(f"\n{'='*70}")
        print(f"FILE: {fname} ({len(pdf.pages)} pages)")
        print(f"{'='*70}")

        result = extract_usage(pdf)
        acct = result["account_usage"]
        lu = result["line_usage"]

        print(f"\nAccount totals: {acct['total_talk_minutes']} min, "
              f"{acct['total_messages']} msgs, {acct['total_data_gb']} GB")

        print(f"\nPer-line usage ({len(lu)} lines):")
        sum_talk = 0
        sum_data_mb = 0.0
        for phone in sorted(lu.keys()):
            u = lu[phone]
            print(f"  {phone:20s}  Talk: {u['talk_minutes']:>6} min  "
                  f"Data: {u['data_mb']:>10.4f} MB  ({u.get('data_gb', 0):.2f} GB)")
            sum_talk += u["talk_minutes"]
            sum_data_mb += u["data_mb"]

        print(f"\n  SUM talk_minutes = {sum_talk} (account total: {acct['total_talk_minutes']})")
        print(f"  SUM data_mb = {sum_data_mb:.2f} MB = {sum_data_mb/1024:.2f} GB "
              f"(account total: {acct['total_data_gb']} GB)")

        if acct["total_talk_minutes"] > 0:
            diff = abs(sum_talk - acct["total_talk_minutes"])
            pct = diff / acct["total_talk_minutes"] * 100
            status = "OK" if pct < 5 else "MISMATCH"
            print(f"  Talk validation: {status} (diff={diff}, {pct:.1f}%)")

        pdf.close()

    # Also test the last bill
    fname = pdfs[-1]
    path = os.path.join(PDF_DIR, fname)
    pdf = pdfplumber.open(path)
    print(f"\n{'='*70}")
    print(f"FILE: {fname} ({len(pdf.pages)} pages)")
    print(f"{'='*70}")
    result = extract_usage(pdf)
    acct = result["account_usage"]
    lu = result["line_usage"]
    print(f"\nAccount totals: {acct['total_talk_minutes']} min, "
          f"{acct['total_messages']} msgs, {acct['total_data_gb']} GB")
    print(f"\nPer-line usage ({len(lu)} lines):")
    sum_talk = 0
    sum_data_mb = 0.0
    for phone in sorted(lu.keys()):
        u = lu[phone]
        print(f"  {phone:20s}  Talk: {u['talk_minutes']:>6} min  "
              f"Data: {u['data_mb']:>10.4f} MB  ({u.get('data_gb', 0):.2f} GB)")
        sum_talk += u["talk_minutes"]
        sum_data_mb += u["data_mb"]
    print(f"\n  SUM talk_minutes = {sum_talk} (account total: {acct['total_talk_minutes']})")
    print(f"  SUM data_mb = {sum_data_mb:.2f} MB = {sum_data_mb/1024:.2f} GB "
          f"(account total: {acct['total_data_gb']} GB)")
    if acct["total_talk_minutes"] > 0:
        diff = abs(sum_talk - acct["total_talk_minutes"])
        pct = diff / acct["total_talk_minutes"] * 100
        status = "OK" if pct < 5 else "MISMATCH"
        print(f"  Talk validation: {status} (diff={diff}, {pct:.1f}%)")
    pdf.close()


if __name__ == "__main__":
    main()
