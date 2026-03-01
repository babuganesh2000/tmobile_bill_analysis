# T-Mobile Bill Analytics Dashboard

A Streamlit-powered analytics dashboard for T-Mobile monthly bills. Upload PDF bills and explore spending trends, per-line charges, savings, and more.

## Features

- **PDF Upload** — Upload monthly T-Mobile bill PDFs directly in the app
- **Account Overview** — KPIs, bill composition, and line counts
- **Monthly Trends** — Track any metric over time with MoM change
- **Line Analysis** — Per-line lifecycle, charge breakdown, and heatmap
- **Person View** — Aggregated cost per person across all lines
- **Savings & Discounts** — AutoPay, service, and device discount tracking
- **Raw Data Explorer** — Run arbitrary SQL against the DuckDB database

## Quick Start (Local)

```bash
# Clone the repo
git clone https://github.com/babuganesh2000/tmobile-bill-analytics.git
cd tmobile-bill-analytics

# Create virtual environment & install deps
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

Open http://localhost:8501 and navigate to **Upload Bills** to add your PDFs.

## Deploy to Streamlit Community Cloud

1. Push this repo to GitHub (already done if you forked it)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click **New app** → select this repo → set main file to `app.py`
4. Deploy!

The app ships with a pre-loaded seed database (`data/tmobile_bills.duckdb`) so the dashboard works immediately. Upload additional PDFs through the UI.

## Project Structure

```
├── app.py                  # Streamlit dashboard (main entry point)
├── parser.py               # Shared PDF parser & DuckDB loader module
├── load_bills.py           # CLI tool for batch-loading PDFs
├── export_xlsx.py          # Export DB to formatted Excel workbook
├── data/
│   └── tmobile_bills.duckdb  # Seed database (pre-loaded)
├── requirements.txt
├── .streamlit/
│   └── config.toml         # Theme & server config
└── README.md
```

## Tech Stack

- **Streamlit** — Web UI
- **Plotly** — Interactive charts
- **DuckDB** — Embedded analytical database
- **pdfplumber** — PDF text extraction
- **Pandas** — Data manipulation

## CLI Usage

For bulk-loading PDFs from a local directory:

```bash
python load_bills.py          # Loads all PDFs in current directory
```

## License

Private — personal use only.
