"""Export all DuckDB tables to a formatted Excel workbook."""

import duckdb
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter

DB_PATH = r"c:\tmobile\tmobile_bills.duckdb"
XLSX_PATH = r"c:\tmobile\TMobile_Bills.xlsx"

HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
HEADER_FILL = PatternFill(start_color="E20074", end_color="E20074", fill_type="solid")  # T-Mobile magenta
HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)
MONEY_FMT = '#,##0.00'
DATE_FMT = 'YYYY-MM-DD'

SHEETS = [
    {
        "name": "Invoices",
        "query": """
            SELECT bill_date, bill_month, total_due, plans_total, equipment_total,
                   services_total, onetime_charges, total_savings, previous_balance,
                   voice_line_count, connected_device_count, wearable_count,
                   data_used_gb, due_date, account_number, file_name
            FROM invoices ORDER BY bill_date
        """,
        "headers": ["Bill Date", "Month", "Total Due", "Plans", "Equipment",
                     "Services", "One-Time", "Total Savings", "Prev Balance",
                     "Voice Lines", "Connected Devices", "Wearables",
                     "Data Used (GB)", "Due Date", "Account", "File"],
        "money_cols": [3,4,5,6,7,8,9,13],
        "date_cols": [1,14],
        "int_cols": [10,11,12],
    },
    {
        "name": "Line Charges",
        "query": """
            SELECT lc.bill_date, lc.phone_number, lc.line_type, lc.status, lc.annotation,
                   lc.plans_charge, lc.equipment_charge, lc.services_charge,
                   lc.onetime_charge, lc.line_total, i.bill_month
            FROM line_charges lc
            JOIN invoices i ON i.file_name = lc.file_name
            ORDER BY lc.bill_date, lc.phone_number
        """,
        "headers": ["Bill Date", "Phone Number", "Line Type", "Status", "Annotation",
                     "Plans", "Equipment", "Services", "One-Time", "Line Total", "Month"],
        "money_cols": [6,7,8,9,10],
        "date_cols": [1],
    },
    {
        "name": "Lines",
        "query": """
            SELECT phone_number, line_type, first_seen_date, last_seen_date,
                   months_active, is_current, lifecycle_notes
            FROM lines ORDER BY line_type, first_seen_date, phone_number
        """,
        "headers": ["Phone Number", "Line Type", "First Seen", "Last Seen",
                     "Months Active", "Current?", "Lifecycle Notes"],
        "date_cols": [3,4],
        "int_cols": [5],
    },
    {
        "name": "Persons",
        "query": """
            SELECT p.person_id, p.person_label, pl.phone_number, pl.relationship,
                   l.line_type, l.first_seen_date, l.last_seen_date,
                   l.months_active, l.is_current
            FROM person_lines pl
            JOIN persons p ON p.person_id = pl.person_id
            JOIN lines l ON l.phone_number = pl.phone_number
            ORDER BY p.person_id
        """,
        "headers": ["Person ID", "Person Label", "Phone Number", "Relationship",
                     "Line Type", "First Seen", "Last Seen", "Months Active", "Current?"],
        "date_cols": [6,7],
        "int_cols": [1,8],
    },
    {
        "name": "Person Monthly Cost",
        "query": """
            SELECT person_id, person_label, bill_date, phone_number, line_type,
                   status, plans_charge, equipment_charge, services_charge,
                   onetime_charge, line_total
            FROM person_monthly_cost
            ORDER BY person_id, bill_date
        """,
        "headers": ["Person ID", "Person", "Bill Date", "Phone", "Line Type",
                     "Status", "Plans", "Equipment", "Services", "One-Time", "Total"],
        "money_cols": [7,8,9,10,11],
        "date_cols": [3],
        "int_cols": [1],
    },
    {
        "name": "Person Totals",
        "query": """
            SELECT person_id, person_label, months,
                   total_plans, total_equipment, total_services,
                   total_onetime, grand_total
            FROM person_total ORDER BY person_id
        """,
        "headers": ["Person ID", "Person", "Months", "Total Plans", "Total Equipment",
                     "Total Services", "Total One-Time", "Grand Total"],
        "money_cols": [4,5,6,7,8],
        "int_cols": [1,3],
    },
    {
        "name": "Savings",
        "query": """
            SELECT s.bill_date, i.bill_month, s.discount_type, s.amount
            FROM savings s
            JOIN invoices i ON i.file_name = s.file_name
            ORDER BY s.bill_date, s.discount_type
        """,
        "headers": ["Bill Date", "Month", "Discount Type", "Amount"],
        "money_cols": [4],
        "date_cols": [1],
    },
    {
        "name": "Payments",
        "query": """
            SELECT p.bill_date, i.bill_month, p.entry_type, p.description, p.amount
            FROM payments p
            JOIN invoices i ON i.file_name = p.file_name
            ORDER BY p.bill_date
        """,
        "headers": ["Bill Date", "Month", "Type", "Description", "Amount"],
        "money_cols": [5],
        "date_cols": [1],
    },
]


def write_sheet(ws, spec, rows):
    headers = spec["headers"]
    money_cols = set(spec.get("money_cols", []))
    date_cols = set(spec.get("date_cols", []))
    int_cols = set(spec.get("int_cols", []))

    # Header row
    for c, hdr in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=hdr)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGN
        cell.border = THIN_BORDER

    # Data rows
    for r_idx, row in enumerate(rows, 2):
        for c_idx, val in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx)
            # Convert booleans to Y/N
            if isinstance(val, bool):
                cell.value = "Y" if val else "N"
                cell.alignment = Alignment(horizontal="center")
            else:
                cell.value = val

            cell.border = THIN_BORDER

            if c_idx in money_cols:
                cell.number_format = MONEY_FMT
                cell.alignment = Alignment(horizontal="right")
            elif c_idx in date_cols:
                cell.number_format = DATE_FMT
            elif c_idx in int_cols:
                cell.alignment = Alignment(horizontal="center")

    # Zebra striping
    stripe_fill = PatternFill(start_color="FFF0F6", end_color="FFF0F6", fill_type="solid")
    for r_idx in range(3, ws.max_row + 1, 2):
        for c_idx in range(1, len(headers) + 1):
            ws.cell(row=r_idx, column=c_idx).fill = stripe_fill

    # Auto-fit column widths
    for c_idx in range(1, len(headers) + 1):
        max_len = len(headers[c_idx - 1])
        for r_idx in range(2, ws.max_row + 1):
            v = ws.cell(row=r_idx, column=c_idx).value
            if v is not None:
                max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(c_idx)].width = min(max_len + 3, 35)

    # Freeze header row
    ws.freeze_panes = "A2"
    # Auto-filter
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"


def main():
    con = duckdb.connect(DB_PATH, read_only=True)
    wb = Workbook()

    # Remove default sheet
    wb.remove(wb.active)

    for spec in SHEETS:
        print(f"  Writing sheet: {spec['name']}")
        rows = con.execute(spec["query"]).fetchall()
        ws = wb.create_sheet(title=spec["name"])
        write_sheet(ws, spec, rows)
        print(f"    {len(rows)} rows")

    con.close()

    wb.save(XLSX_PATH)
    print(f"\nSaved: {XLSX_PATH}")


if __name__ == "__main__":
    main()
