"""Quick test to extract usage details from a bill PDF."""
import pdfplumber, re

pdf = pdfplumber.open(r"c:\tmobile\81559770-0952590898_124.pdf")
print(f"Total pages: {len(pdf.pages)}")

# 1) Parse per-line data from YOU USED summary (pages 2-3)
print("\n=== PER-LINE DATA USAGE (from summary) ===")
for i in [1, 2]:
    text = pdf.pages[i].extract_text() or ""
    for m in re.finditer(r"\((\d{3})\)\s*(\d{3}-\d{4})\s+([\d.]+)GB", text):
        print(f"  ({m.group(1)}) {m.group(2)}: {m.group(3)} GB")

# Total talk/messages
print("\n=== ACCOUNT-LEVEL TOTALS (from summary) ===")
for i in [1, 2]:
    text = pdf.pages[i].extract_text() or ""
    for m in re.finditer(r"([\d,]+)\s+minutes\s+of\s+talk", text):
        print(f"  Total talk minutes: {m.group(1)}")
    for m in re.finditer(r"([\d,]+)\s+messages", text):
        print(f"  Total messages: {m.group(1)}")

# 2) Scan USAGE DETAILS pages for per-phone talk/text/data totals
print("\n=== USAGE DETAIL SECTIONS ===")
for i in range(4, len(pdf.pages)):
    text = pdf.pages[i].extract_text() or ""
    
    # Phone headers with date range
    for m in re.finditer(r"\((\d{3})\)\s*(\d{3}-\d{4})\s+(\w+ \d+\s*-\s*\w+ \d+)", text):
        print(f"Page {i+1}: Phone header: ({m.group(1)}) {m.group(2)} [{m.group(3).strip()}]")
    
    # CONTINUED sections
    for m in re.finditer(r"CONTINUED\s*-\s*\((\d{3})\)\s*(\d{3}-\d{4})\s*,\s*(\w+)", text):
        print(f"Page {i+1}: Continued: ({m.group(1)}) {m.group(2)} - {m.group(3)}")
    
    # Totals with number and cost
    for m in re.finditer(r"Totals\s+([\d,]+\.?\d*)\s+\$([\d,.]+)", text):
        print(f"Page {i+1}: Totals: {m.group(1)} ${m.group(2)}")
    
    # Totals with cost only
    for m in re.finditer(r"(?<!\d)Totals\s+\$([\d,.]+)", text):
        print(f"Page {i+1}: Totals (cost only): ${m.group(1)}")

pdf.close()
