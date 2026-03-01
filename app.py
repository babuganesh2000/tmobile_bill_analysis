"""
T-Mobile Bill Analytics  --  Streamlit Dashboard
Supports both local .duckdb seed and in-app PDF upload.
Deployable on Streamlit Community Cloud.
"""

import os, io, tempfile, shutil
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from parser import extract_bill, init_schema, load_bill_data, rebuild_dimensions

# ─── Config ─────────────────────────────────────────────────────────

SEED_DB = os.path.join(os.path.dirname(__file__), "data", "tmobile_bills.duckdb")

st.set_page_config(
    page_title="T-Mobile Bill Analytics",
    page_icon=":iphone:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Session-level DB ───────────────────────────────────────────────

def _get_db_path() -> str:
    """Return the path to the session's writable DuckDB file."""
    if "db_path" not in st.session_state:
        tmp_dir = tempfile.mkdtemp(prefix="tmobile_")
        tmp_db = os.path.join(tmp_dir, "bills.duckdb")
        if os.path.exists(SEED_DB):
            shutil.copy2(SEED_DB, tmp_db)
        st.session_state["db_path"] = tmp_db
    return st.session_state["db_path"]


def get_con() -> duckdb.DuckDBPyConnection:
    """Return the session's DuckDB connection (read-write)."""
    if "db_con" not in st.session_state or st.session_state["db_con"] is None:
        path = _get_db_path()
        con = duckdb.connect(path)
        init_schema(con)
        st.session_state["db_con"] = con
    return st.session_state["db_con"]


def run_query(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).fetchdf()


def has_data() -> bool:
    try:
        n = get_con().execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
        return n > 0
    except Exception:
        return False


# ─── Sidebar ────────────────────────────────────────────────────────

st.sidebar.markdown(
    """
    <div style="text-align:center">
        <h1 style="color:#E20074">T-Mobile</h1>
        <p style="font-size:0.9em">Bill Analytics Dashboard</p>
    </div>
    """,
    unsafe_allow_html=True,
)

page = st.sidebar.radio(
    "Navigate",
    [
        "Upload Bills",
        "Overview",
        "Monthly Trends",
        "Line Analysis",
        "Person View",
        "Savings & Discounts",
        "Raw Data Explorer",
    ],
)


# ════════════════════════════════════════════════════════════════════
#  PAGE: Upload Bills
# ════════════════════════════════════════════════════════════════════

if page == "Upload Bills":
    st.title("Upload T-Mobile Bill PDFs")
    st.markdown(
        "Upload one or more T-Mobile bill PDFs. They will be parsed and added "
        "to the analytics database. Duplicate bills (same filename) are safely "
        "replaced."
    )

    uploaded = st.file_uploader(
        "Choose PDF files",
        type=["pdf"],
        accept_multiple_files=True,
    )

    if uploaded:
        if st.button("Process Uploaded Bills", type="primary"):
            con = get_con()
            results = []
            progress = st.progress(0, text="Processing...")

            for i, f in enumerate(uploaded):
                progress.progress((i + 1) / len(uploaded), text=f"Processing {f.name}...")
                try:
                    buf = io.BytesIO(f.read())
                    data = extract_bill(buf, file_name=f.name)
                    load_bill_data(con, data)
                    inv = data["invoice"]
                    results.append({
                        "File": f.name,
                        "Bill Date": inv["bill_date"],
                        "Month": inv["bill_month"],
                        "Total Due": f"${inv['total_due']:,.2f}",
                        "Lines": len(data["lines"]),
                        "Status": "Loaded",
                    })
                except Exception as e:
                    results.append({
                        "File": f.name, "Bill Date": "-", "Month": "-",
                        "Total Due": "-", "Lines": 0, "Status": f"Error: {e}",
                    })

            # Rebuild dimensions after all uploads
            rebuild_dimensions(con)
            progress.empty()

            st.success(f"Processed {len(uploaded)} file(s)")
            st.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)

    st.divider()

    # Show current database state
    if has_data():
        st.subheader("Bills Currently in Database")
        st.dataframe(
            run_query("""
                SELECT bill_date, bill_month, total_due, voice_line_count,
                       connected_device_count, wearable_count, file_name
                FROM invoices ORDER BY bill_date
            """),
            width="stretch",
            hide_index=True,
        )
        n = run_query("SELECT COUNT(*) AS n FROM invoices").iloc[0, 0]
        st.caption(f"{n} bill(s) loaded")

        st.divider()
        st.subheader("Reset Database")
        st.caption("Delete **all** loaded bills and start from scratch with an empty database.")
        if st.button("Delete All Data & Reset", type="secondary"):
            st.session_state["confirm_reset"] = True

        if st.session_state.get("confirm_reset"):
            st.warning("This will permanently delete every bill from the local database. Are you sure?")
            c1, c2 = st.columns(2)
            if c1.button("Yes, delete everything", type="primary"):
                # Close existing connection
                con = get_con()
                con.close()
                st.session_state["db_con"] = None
                # Remove old DB file and create a fresh one
                db_path = st.session_state["db_path"]
                if os.path.exists(db_path):
                    os.remove(db_path)
                new_con = duckdb.connect(db_path)
                init_schema(new_con)
                st.session_state["db_con"] = new_con
                st.session_state.pop("confirm_reset", None)
                st.success("Database reset to empty. Upload new PDFs to begin.")
                st.rerun()
            if c2.button("Cancel"):
                st.session_state.pop("confirm_reset", None)
                st.rerun()
    else:
        st.info("No bills loaded yet. Upload PDFs above to get started.")


# ─── Guard: remaining pages require data ────────────────────────────

if page != "Upload Bills" and not has_data():
    st.warning("No data loaded. Go to **Upload Bills** to add PDFs first.")
    st.stop()

# ─── Load dataframes (only when data exists) ────────────────────────

if page != "Upload Bills":
    invoices = run_query("SELECT * FROM invoices ORDER BY bill_date")
    line_charges = run_query("""
        SELECT lc.*, i.bill_month
        FROM line_charges lc
        JOIN invoices i ON i.file_name = lc.file_name
        ORDER BY lc.bill_date, lc.phone_number
    """)
    lines_dim = run_query("SELECT * FROM lines ORDER BY line_type, phone_number")
    persons = run_query("""
        SELECT p.person_id, p.person_label, pl.phone_number, pl.relationship,
               l.line_type, l.first_seen_date, l.last_seen_date,
               l.months_active, l.is_current
        FROM person_lines pl
        JOIN persons p ON p.person_id = pl.person_id
        JOIN lines l ON l.phone_number = pl.phone_number
        ORDER BY p.person_id
    """)
    savings = run_query("""
        SELECT s.*, i.bill_month
        FROM savings s JOIN invoices i ON i.file_name = s.file_name
        ORDER BY s.bill_date
    """)
    payments = run_query("""
        SELECT p.*, i.bill_month
        FROM payments p JOIN invoices i ON i.file_name = p.file_name
        ORDER BY p.bill_date
    """)
    person_monthly = run_query("SELECT * FROM person_monthly_cost ORDER BY person_id, bill_date")
    person_totals = run_query("SELECT * FROM person_total ORDER BY person_id")

    invoices["label"] = invoices["bill_date"].apply(
        lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
    )
    num_months = len(invoices)


# ════════════════════════════════════════════════════════════════════
#  PAGE: Overview
# ════════════════════════════════════════════════════════════════════

if page == "Overview":
    st.title("Account Overview")

    total_spent = float(invoices["total_due"].sum())
    avg_bill = float(invoices["total_due"].mean())
    latest = invoices.iloc[-1]
    total_saved = float(invoices["total_savings"].sum())
    active_lines = int(lines_dim["is_current"].sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"Total Spent ({num_months} mo)", f"${total_spent:,.2f}")
    c2.metric("Avg Monthly Bill", f"${avg_bill:,.2f}")
    c3.metric("Latest Bill", f"${float(latest['total_due']):,.2f}", f"{latest['bill_month']}")
    c4.metric("Total Savings", f"${total_saved:,.2f}")
    c5.metric("Active Lines", active_lines, f"of {len(lines_dim)} total")

    st.divider()

    fig = px.bar(
        invoices, x="label", y="total_due",
        color_discrete_sequence=["#E20074"],
        labels={"label": "Month", "total_due": "Total Due ($)"},
        title="Monthly Bill Total",
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, width='stretch')

    comp = invoices[["label", "plans_total", "equipment_total", "services_total", "onetime_charges"]].copy()
    comp = comp.melt(id_vars="label", var_name="Category", value_name="Amount")
    comp["Category"] = comp["Category"].map({
        "plans_total": "Plans", "equipment_total": "Equipment",
        "services_total": "Services", "onetime_charges": "One-Time",
    })
    fig2 = px.bar(
        comp, x="label", y="Amount", color="Category",
        title="Bill Composition by Category",
        labels={"label": "Month", "Amount": "Amount ($)"},
        color_discrete_map={
            "Plans": "#E20074", "Equipment": "#5C2D91",
            "Services": "#0078D4", "One-Time": "#FFB900",
        },
    )
    fig2.update_layout(xaxis_tickangle=-45, barmode="stack")
    st.plotly_chart(fig2, width='stretch')

    fig3 = px.line(
        invoices, x="label",
        y=["voice_line_count", "connected_device_count", "wearable_count"],
        title="Line Counts Over Time",
        labels={"label": "Month", "value": "Count", "variable": "Type"},
        markers=True,
    )
    fig3.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig3, width='stretch')


# ════════════════════════════════════════════════════════════════════
#  PAGE: Monthly Trends
# ════════════════════════════════════════════════════════════════════

elif page == "Monthly Trends":
    st.title("Monthly Trends")

    metric = st.selectbox(
        "Select metric",
        ["total_due", "plans_total", "equipment_total", "services_total",
         "onetime_charges", "total_savings", "data_used_gb"],
        format_func=lambda x: x.replace("_", " ").title(),
    )

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=invoices["label"], y=invoices[metric],
               name=metric.replace("_", " ").title(), marker_color="#E20074"),
        secondary_y=False,
    )
    invoices["_mom"] = invoices[metric].pct_change() * 100
    fig.add_trace(
        go.Scatter(x=invoices["label"], y=invoices["_mom"], name="MoM %",
                   mode="lines+markers", marker_color="#5C2D91"),
        secondary_y=True,
    )
    fig.update_layout(
        title=f"{metric.replace('_',' ').title()} -- Trend & Month-over-Month Change",
        xaxis_tickangle=-45,
    )
    fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
    fig.update_yaxes(title_text="MoM Change (%)", secondary_y=True)
    st.plotly_chart(fig, width='stretch')

    col1, col2, col3, col4 = st.columns(4)
    s = invoices[metric]
    col1.metric("Min", f"${float(s.min()):,.2f}")
    col2.metric("Max", f"${float(s.max()):,.2f}")
    col3.metric("Avg", f"${float(s.mean()):,.2f}")
    col4.metric("Std Dev", f"${float(s.std()):,.2f}")

    st.subheader("Data Table")
    st.dataframe(
        invoices[["bill_date", "bill_month", metric, "_mom"]].rename(columns={"_mom": "MoM %"}),
        width='stretch', hide_index=True,
    )


# ════════════════════════════════════════════════════════════════════
#  PAGE: Line Analysis
# ════════════════════════════════════════════════════════════════════

elif page == "Line Analysis":
    st.title("Line-by-Line Analysis")

    tab1, tab2, tab3 = st.tabs(["Line Lifecycle", "Per-Line Charges", "Heatmap"])

    with tab1:
        st.subheader("Line Lifecycle")
        st.dataframe(
            lines_dim[["phone_number", "line_type", "first_seen_date", "last_seen_date",
                        "months_active", "is_current", "lifecycle_notes"]],
            width='stretch', hide_index=True,
        )
        timeline = lines_dim.copy()
        timeline["first_seen_date"] = pd.to_datetime(timeline["first_seen_date"])
        timeline["last_seen_date"] = pd.to_datetime(timeline["last_seen_date"])
        fig = px.timeline(
            timeline, x_start="first_seen_date", x_end="last_seen_date",
            y="phone_number", color="line_type",
            title="Line Presence Timeline",
            color_discrete_map={"Voice": "#E20074", "Mobile Internet": "#0078D4", "Wearable": "#FFB900"},
        )
        fig.update_layout(yaxis_title="Phone Number", xaxis_title="Date", height=600)
        st.plotly_chart(fig, width='stretch')

    with tab2:
        st.subheader("Per-Line Monthly Charges")
        phone_list = sorted(line_charges[line_charges["phone_number"] != "Account"]["phone_number"].unique())
        selected_phone = st.selectbox("Select phone number", phone_list)

        ph_data = line_charges[line_charges["phone_number"] == selected_phone].copy()
        ph_data["label"] = ph_data["bill_date"].apply(
            lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
        )
        fig = px.bar(
            ph_data, x="label", y="line_total", color="status",
            title=f"Monthly Total -- {selected_phone}",
            labels={"label": "Month", "line_total": "Total ($)"},
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, width='stretch')

        melt = ph_data[["label", "plans_charge", "equipment_charge", "services_charge", "onetime_charge"]].melt(
            id_vars="label", var_name="Category", value_name="Amount"
        )
        melt["Category"] = melt["Category"].str.replace("_charge", "").str.title()
        fig2 = px.bar(melt, x="label", y="Amount", color="Category",
                       title=f"Charge Breakdown -- {selected_phone}", barmode="stack")
        fig2.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig2, width='stretch')

        st.dataframe(ph_data, width='stretch', hide_index=True)

    with tab3:
        st.subheader("Monthly Charge Heatmap")
        heat_data = line_charges[line_charges["phone_number"] != "Account"].copy()
        heat_data["month"] = heat_data["bill_date"].apply(
            lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
        )
        pivot = heat_data.pivot_table(index="phone_number", columns="month", values="line_total", aggfunc="sum")
        month_order = invoices["label"].tolist()
        pivot = pivot.reindex(columns=[m for m in month_order if m in pivot.columns])
        fig = px.imshow(
            pivot.fillna(0).values,
            x=pivot.columns.tolist(), y=pivot.index.tolist(),
            color_continuous_scale="Magma", aspect="auto",
            title="Charge Heatmap (Phone x Month)",
            labels=dict(x="Month", y="Phone", color="$ Amount"),
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, width='stretch')


# ════════════════════════════════════════════════════════════════════
#  PAGE: Person View
# ════════════════════════════════════════════════════════════════════

elif page == "Person View":
    st.title("Person-Level Analysis")

    tab1, tab2 = st.tabs(["Totals", "Monthly Drill-down"])

    with tab1:
        st.subheader("Total Cost per Person (All Months)")
        fig = px.bar(
            person_totals.sort_values("grand_total", ascending=True),
            x="grand_total", y="person_label", orientation="h",
            color="grand_total", color_continuous_scale=["#FFB900", "#E20074"],
            title="Total Cost by Person",
            labels={"grand_total": "Total ($)", "person_label": "Person"},
        )
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, width='stretch')

        st.dataframe(person_totals, width='stretch', hide_index=True)

        st.subheader("Person - Phone Mapping")
        st.dataframe(persons, width='stretch', hide_index=True)

    with tab2:
        st.subheader("Monthly Cost per Person")
        person_list = person_totals["person_label"].tolist()
        sel_person = st.selectbox("Select person", person_list)

        p_data = person_monthly[person_monthly["person_label"] == sel_person].copy()
        p_data["month"] = p_data["bill_date"].apply(
            lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
        )
        monthly_agg = p_data.groupby("month", sort=False).agg(
            total=("line_total", "sum"), plans=("plans_charge", "sum"),
            equipment=("equipment_charge", "sum"), services=("services_charge", "sum"),
            onetime=("onetime_charge", "sum"),
        ).reset_index()

        fig = px.bar(
            monthly_agg, x="month", y="total",
            color_discrete_sequence=["#E20074"],
            title=f"Monthly Total - {sel_person}",
            labels={"month": "Month", "total": "Total ($)"},
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, width='stretch')

        cats = {
            "Plans": float(p_data["plans_charge"].sum()),
            "Equipment": float(p_data["equipment_charge"].sum()),
            "Services": float(p_data["services_charge"].sum()),
            "One-Time": float(p_data["onetime_charge"].sum()),
        }
        cats = {k: v for k, v in cats.items() if v != 0}
        if cats:
            fig2 = px.pie(
                names=list(cats.keys()), values=list(cats.values()),
                title=f"Charge Mix - {sel_person}",
                color_discrete_sequence=["#E20074", "#5C2D91", "#0078D4", "#FFB900"],
            )
            st.plotly_chart(fig2, width='stretch')

        st.dataframe(p_data, width='stretch', hide_index=True)


# ════════════════════════════════════════════════════════════════════
#  PAGE: Savings & Discounts
# ════════════════════════════════════════════════════════════════════

elif page == "Savings & Discounts":
    st.title("Savings & Discounts")

    tab1, tab2 = st.tabs(["Discounts", "Payments & Credits"])

    with tab1:
        savings["label"] = savings["bill_date"].apply(
            lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
        )
        fig = px.bar(
            savings, x="label", y="amount", color="discount_type", barmode="group",
            title="Monthly Discounts by Type",
            labels={"label": "Month", "amount": "Amount ($)"},
            color_discrete_map={
                "AutoPay discounts": "#E20074",
                "Service discounts": "#0078D4",
                "Device discounts": "#5C2D91",
            },
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, width='stretch')

        pivot = savings.pivot_table(index="label", columns="discount_type", values="amount", aggfunc="sum").fillna(0)
        month_order = invoices["label"].tolist()
        pivot = pivot.reindex([m for m in month_order if m in pivot.index])
        fig2 = px.area(
            pivot.reset_index(), x="label", y=pivot.columns.tolist(),
            title="Cumulative Savings Over Time",
            labels={"label": "Month", "value": "Amount ($)", "variable": "Type"},
        )
        fig2.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig2, width='stretch')

        total_savings = float(savings["amount"].sum())
        st.metric(f"Total Discounts ({num_months} months)", f"${total_savings:,.2f}")
        st.dataframe(savings, width='stretch', hide_index=True)

    with tab2:
        if len(payments) > 0:
            st.subheader("Payments & Credits")
            st.dataframe(payments, width='stretch', hide_index=True)
            fig = px.bar(
                payments, x="bill_date", y="amount", color="entry_type",
                title="Payments & Credits", labels={"amount": "Amount ($)"},
                color_discrete_map={"payment": "#E20074", "credit": "#0078D4"},
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No payment/credit records found.")


# ════════════════════════════════════════════════════════════════════
#  PAGE: Raw Data Explorer
# ════════════════════════════════════════════════════════════════════

elif page == "Raw Data Explorer":
    st.title("Raw Data Explorer")
    st.markdown("Run any SQL query against the DuckDB database.")

    default_sql = "SELECT * FROM invoices ORDER BY bill_date"
    sql = st.text_area("SQL Query", value=default_sql, height=120)

    if st.button("Run Query", type="primary"):
        try:
            result = run_query(sql)
            st.success(f"{len(result)} rows returned")
            st.dataframe(result, width='stretch', hide_index=True)
            csv = result.to_csv(index=False)
            st.download_button("Download CSV", csv, "query_result.csv", "text/csv")
        except Exception as e:
            st.error(f"Query error: {e}")

    st.divider()
    st.subheader("Available Tables & Views")
    tables_info = {
        "invoices": "Monthly bill totals",
        "line_charges": "Per-phone charges per bill",
        "lines": "Phone number dimension with lifecycle",
        "persons": "Person mapping (Person 1..N)",
        "person_lines": "Phone-to-person bridge",
        "savings": "Discount breakdown per bill",
        "payments": "Payments & credits",
        "person_monthly_cost": "View: charges joined to persons by month",
        "person_total": "View: aggregated per-person totals",
    }
    for tbl, desc in tables_info.items():
        st.code(f"{tbl}  --  {desc}", language=None)

