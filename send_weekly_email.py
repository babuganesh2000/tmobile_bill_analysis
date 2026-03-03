"""
send_weekly_email.py  –  Standalone script for GitHub Actions cron.

Connects to MotherDuck, computes per-person balances,
and sends a master summary to Sathish (+ optional individual reminders)
via the SendGrid v3 API.

Environment variables required:
    MOTHERDUCK_TOKEN   – MotherDuck PAT token
    SENDGRID_API_KEY   – SendGrid API key
    SENDGRID_FROM_EMAIL – Verified sender email (default: sathishnb4u@gmail.com)
"""

import os
import sys
import json
import requests
import duckdb
import pandas as pd
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────

ACCT_HOLDER = "Sathish"
APP_URL = "https://tmobilebillanalysis-w8t9cas3jwk6u4cc6qqwea.streamlit.app"
PLAN_PREMIUM = 10.00

MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
SG_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
SG_FROM_EMAIL = os.environ.get("SENDGRID_FROM_EMAIL", "sathishnb4u@gmail.com")


def get_con():
    """Return a DuckDB connection to MotherDuck."""
    if not MD_TOKEN:
        print("ERROR: MOTHERDUCK_TOKEN not set.")
        sys.exit(1)
    return duckdb.connect(f"md:tmobile_bills?motherduck_token={MD_TOKEN}")


# ── Reuse the same account-share logic from app.py ──────────────────

def compute_acct_shares(lc_month: pd.DataFrame) -> dict:
    """Return {phone_number: acct_share} for one billing month."""
    acct_rows = lc_month[lc_month["phone_number"] == "Account"]
    indiv = lc_month[lc_month["phone_number"] != "Account"]
    active = indiv[~indiv["status"].isin(["removed", "cancelled"])]
    acct_total = float(acct_rows["line_total"].sum()) if not acct_rows.empty else 0.0

    voice_active = active[active["line_type"] == "Voice"]
    n_voice = len(voice_active)
    discounted_phones = set(
        voice_active[voice_active["plans_charge"] < PLAN_PREMIUM]["phone_number"].tolist()
    )
    n_discounted = len(discounted_phones)
    premium_pool = PLAN_PREMIUM * n_discounted
    remaining_pool = acct_total - premium_pool
    equal_share = round(remaining_pool / n_voice, 2) if n_voice > 0 else 0.0
    equal_remainder = round(remaining_pool - equal_share * n_voice, 2)

    shares = {}
    voice_idx = 0
    for phone in active["phone_number"].tolist():
        lt = active.loc[active["phone_number"] == phone, "line_type"].iloc[0]
        if lt == "Voice":
            voice_idx += 1
            premium = PLAN_PREMIUM if phone in discounted_phones else 0.0
            sh = premium + equal_share
            if voice_idx == n_voice:
                sh += equal_remainder
            shares[phone] = round(sh, 2)
        else:
            shares[phone] = 0.0
    return shares


def build_month_splitup(lc_month: pd.DataFrame) -> pd.DataFrame:
    """Build per-line splitup for one month with account share + taxes."""
    shares = compute_acct_shares(lc_month)
    indiv = lc_month[lc_month["phone_number"] != "Account"]
    active = indiv[~indiv["status"].isin(["removed", "cancelled"])]
    rows = []
    for _, r in active.iterrows():
        phone = r["phone_number"]
        plan = float(r["plans_charge"])
        equip = float(r["equipment_charge"])
        svc = float(r["services_charge"])
        ot = float(r["onetime_charge"])
        total = float(r["line_total"])
        taxes = round(total - plan - equip - svc - ot, 2)
        acct_sh = shares.get(phone, 0.0)
        rows.append({
            "phone_number": phone,
            "line_type": r["line_type"],
            "plan": plan, "equipment": equip, "services": svc,
            "onetime": ot, "taxes_fees": taxes,
            "line_total": total, "acct_share": acct_sh,
            "month_total": round(total + acct_sh, 2),
        })
    return pd.DataFrame(rows)


# ── Email builders ──────────────────────────────────────────────────

def build_master_html(balance_df: pd.DataFrame) -> str:
    rows_html = ""
    for _, r in balance_df.iterrows():
        bg = ' style="background:#FFF0F5"' if r["balance"] > 0 else ""
        rows_html += f"""<tr{bg}>
            <td style="padding:6px;border:1px solid #ddd">{r['person_label']}</td>
            <td style="padding:6px;border:1px solid #ddd;text-align:right">${r['total_owed']:,.2f}</td>
            <td style="padding:6px;border:1px solid #ddd;text-align:right">${r['total_paid']:,.2f}</td>
            <td style="padding:6px;border:1px solid #ddd;text-align:right;font-weight:bold">${r['balance']:,.2f}</td>
        </tr>"""
    total_out = balance_df["balance"].sum()
    return f"""
    <div style="font-family:Arial,sans-serif;max-width:650px;margin:auto">
      <h2 style="color:#E20074">T-Mobile Bill – Weekly Balance Summary</h2>
      <p>Hi <strong>{ACCT_HOLDER}</strong>,</p>
      <p>Here is the current balance summary for all members:</p>
      <table style="border-collapse:collapse;width:100%">
        <tr style="background:#E20074;color:white">
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Person</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:right">Total Owed</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:right">Total Paid</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:right">Balance Due</th>
        </tr>
        {rows_html}
        <tr style="background:#333;color:white">
          <td style="padding:8px;border:1px solid #ddd"><strong>TOTAL</strong></td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${balance_df['total_owed'].sum():,.2f}</strong></td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${balance_df['total_paid'].sum():,.2f}</strong></td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${total_out:,.2f}</strong></td>
        </tr>
      </table>
      <p style="margin-top:16px"><a href="{APP_URL}">Open Dashboard →</a></p>
      <p style="color:#888;font-size:12px">Sent automatically by T-Mobile Bill Tracker · {datetime.now().strftime('%b %d, %Y')}</p>
    </div>"""


def build_person_html(person: str, row: pd.Series) -> str:
    return f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
      <h2 style="color:#E20074">T-Mobile Bill – Balance Reminder</h2>
      <p>Hi <strong>{person}</strong>,</p>
      <p>This is a friendly reminder about your T-Mobile bill share.</p>
      <table style="border-collapse:collapse;width:100%">
        <tr><td style="padding:8px;border:1px solid #ddd"><strong>Total Owed</strong></td>
            <td style="padding:8px;border:1px solid #ddd;text-align:right">${row['total_owed']:,.2f}</td></tr>
        <tr><td style="padding:8px;border:1px solid #ddd"><strong>Total Paid</strong></td>
            <td style="padding:8px;border:1px solid #ddd;text-align:right">${row['total_paid']:,.2f}</td></tr>
        <tr style="background:#FFF0F5"><td style="padding:8px;border:1px solid #ddd"><strong>Balance Due</strong></td>
            <td style="padding:8px;border:1px solid #ddd;text-align:right;color:#E20074;font-weight:bold">${row['balance']:,.2f}</td></tr>
      </table>
      <p style="margin-top:16px">If you've already made a payment, please disregard this.<br>
      Please reach out to <strong>{ACCT_HOLDER}</strong> for payment details.</p>
      <p style="color:#888;font-size:12px">Sent automatically by <a href="{APP_URL}">T-Mobile Bill Tracker</a> · {datetime.now().strftime('%b %d, %Y')}</p>
    </div>"""


# ── SendGrid sender ────────────────────────────────────────────────

def send_email(to_email: str, to_name: str, subject: str, html: str) -> bool:
    """Send email via SendGrid v3 API. Returns True on success."""
    if not SG_API_KEY:
        print(f"  SKIP {to_name}: SENDGRID_API_KEY not set")
        return False
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SG_API_KEY}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": to_email, "name": to_name}]}],
            "from": {"email": SG_FROM_EMAIL, "name": "T-Mobile Bill Tracker"},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        },
        timeout=15,
    )
    if resp.status_code in (200, 201, 202):
        print(f"  ✅ {to_name} ({to_email}): sent")
        return True
    print(f"  ❌ {to_name}: {resp.status_code} – {resp.text[:200]}")
    return False


# ── Main ────────────────────────────────────────────────────────────

def main():
    print(f"=== T-Mobile Weekly Email – {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    con = get_con()

    # Load line_charges with bill_month
    line_charges = con.execute("""
        SELECT lc.*, i.bill_month
        FROM line_charges lc
        JOIN invoices i ON i.file_name = lc.file_name
        ORDER BY lc.bill_date, lc.phone_number
    """).fetchdf()

    person_monthly = con.execute(
        "SELECT * FROM person_monthly_cost ORDER BY person_id, bill_date"
    ).fetchdf()

    persons = con.execute("""
        SELECT p.person_id, p.person_label, pl.phone_number
        FROM person_lines pl
        JOIN persons p ON p.person_id = pl.person_id
    """).fetchdf()

    name_map = (
        persons.drop_duplicates("phone_number")
        .set_index("phone_number")["person_label"].to_dict()
    )

    # Build splitup for all months
    all_rows = []
    for bd in line_charges["bill_date"].unique():
        lc_m = line_charges[line_charges["bill_date"] == bd]
        sp = build_month_splitup(lc_m)
        sp["bill_date"] = bd
        all_rows.append(sp)
    bal_all = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    if bal_all.empty:
        print("No data — nothing to send.")
        return

    # Map phone → person
    pm_map = person_monthly[["phone_number", "bill_date", "person_label"]].drop_duplicates()
    bal_all = bal_all.merge(pm_map, on=["phone_number", "bill_date"], how="left")
    bal_all["person_label"] = bal_all["person_label"].fillna(
        bal_all["phone_number"].map(name_map)
    ).fillna(bal_all["phone_number"])

    # Per-person owed (exclude account holder)
    person_owed = (
        bal_all[bal_all["person_label"] != ACCT_HOLDER]
        .groupby("person_label")["month_total"].sum()
        .reset_index()
        .rename(columns={"month_total": "total_owed"})
    )

    # Settlements
    stl = con.execute("SELECT * FROM settlements").fetchdf()
    if not stl.empty:
        person_paid = (
            stl.groupby("person_label")["amount"].sum()
            .reset_index()
            .rename(columns={"amount": "total_paid"})
        )
    else:
        person_paid = pd.DataFrame(columns=["person_label", "total_paid"])

    balance_df = person_owed.merge(person_paid, on="person_label", how="left")
    balance_df["total_paid"] = balance_df["total_paid"].fillna(0).infer_objects(copy=False)
    balance_df["balance"] = (balance_df["total_owed"] - balance_df["total_paid"]).round(2)
    balance_df = balance_df.sort_values("balance", ascending=False).reset_index(drop=True)

    # Emails
    pe = con.execute("SELECT * FROM person_emails").fetchdf()
    email_map = dict(zip(pe["person_label"], pe["email"])) if not pe.empty else {}

    print(f"\nBalance summary ({len(balance_df)} persons):")
    for _, r in balance_df.iterrows():
        print(f"  {r['person_label']}: owed=${r['total_owed']:,.2f}  paid=${r['total_paid']:,.2f}  balance=${r['balance']:,.2f}")

    total_outstanding = balance_df["balance"].sum()
    print(f"\nTotal outstanding: ${total_outstanding:,.2f}")

    # ── Send master summary to Sathish ────────────────────────────
    sathish_email = email_map.get(ACCT_HOLDER)
    sent = 0
    if sathish_email:
        print(f"\nSending master summary to {ACCT_HOLDER} ({sathish_email})...")
        if send_email(sathish_email, ACCT_HOLDER,
                      "T-Mobile Bill – Weekly Balance Summary",
                      build_master_html(balance_df)):
            sent += 1
    else:
        print(f"\n⚠️  No email configured for {ACCT_HOLDER} — skipping master summary.")

    # ── Send individual reminders to those with balance > 0 ───────
    print("\nSending individual reminders...")
    for _, r in balance_df.iterrows():
        p = r["person_label"]
        if r["balance"] <= 0:
            print(f"  SKIP {p}: balance is ${r['balance']:,.2f}")
            continue
        if p not in email_map:
            print(f"  SKIP {p}: no email configured")
            continue
        if send_email(email_map[p], p,
                      "T-Mobile Bill – Balance Reminder",
                      build_person_html(p, r)):
            sent += 1

    print(f"\nDone. {sent} email(s) sent.")
    con.close()


if __name__ == "__main__":
    main()
