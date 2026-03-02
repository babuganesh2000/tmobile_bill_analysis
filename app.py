"""
T-Mobile Bill Analytics  --  Streamlit Dashboard
Supports both local .duckdb seed and in-app PDF upload.
Deployable on Streamlit Community Cloud.
"""

import sys, os, io, json, base64
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from parser import extract_bill, init_schema, load_bill_data, rebuild_dimensions

# ─── Config ─────────────────────────────────────────────────────────

SEED_DB = os.path.join(os.path.dirname(__file__), "data", "tmobile_bills.duckdb")
AUTH_DB = os.path.join(os.path.dirname(__file__), "data", "auth.duckdb")

def _motherduck_token() -> str:
    """Return the MotherDuck token from secrets, or empty string if not set."""
    try:
        tok = st.secrets.get("motherduck", {}).get("token", "")
        return tok if tok and "YOUR_" not in tok else ""
    except Exception:
        return ""

def _is_motherduck() -> bool:
    return bool(_motherduck_token())

@st.cache_resource
def _ensure_md_databases():
    """Create MotherDuck databases if they don't exist (runs once per process)."""
    tok = _motherduck_token()
    if not tok:
        return
    con = duckdb.connect(f"md:?motherduck_token={tok}")
    con.execute("CREATE DATABASE IF NOT EXISTS tmobile_bills")
    con.execute("CREATE DATABASE IF NOT EXISTS tmobile_auth")
    con.close()

st.set_page_config(
    page_title="T-Mobile Bill Analytics",
    page_icon=":iphone:",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── Google OAuth Authentication ────────────────────────────────────

def _google_auth_enabled() -> bool:
    """Return True if Google OAuth secrets are configured with real values."""
    try:
        s = st.secrets.get("google_auth", {})
        cid = s.get("client_id", "")
        csec = s.get("client_secret", "")
        # Skip if empty or still placeholder values
        if not cid or not csec:
            return False
        if "YOUR_" in cid or "YOUR_" in csec:
            return False
        return True
    except Exception:
        return False


def _init_allowed_users_table(con):
    """Create the allowed_users table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            email       VARCHAR PRIMARY KEY,
            name        VARCHAR,
            role        VARCHAR DEFAULT 'viewer',
            added_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _is_user_allowed(con, email: str) -> bool:
    """Check if the email is in the allowed_users table."""
    n = con.execute(
        "SELECT COUNT(*) FROM allowed_users WHERE LOWER(email) = LOWER(?)", [email]
    ).fetchone()[0]
    return n > 0


def _get_user_role(con, email: str) -> str:
    """Return the role of the user ('admin' or 'viewer')."""
    row = con.execute(
        "SELECT role FROM allowed_users WHERE LOWER(email) = LOWER(?)", [email]
    ).fetchone()
    return row[0] if row else "viewer"


def _get_allowed_user_count(con) -> int:
    return con.execute("SELECT COUNT(*) FROM allowed_users").fetchone()[0]


def login_gate():
    """Google OAuth login gate with PKCE — blocks access until authenticated."""
    if not _google_auth_enabled():
        return

    if st.session_state.get("authenticated"):
        return

    import urllib.parse, urllib.request, urllib.error

    client_id = st.secrets["google_auth"]["client_id"]
    client_secret = st.secrets["google_auth"]["client_secret"]
    redirect_uri = st.secrets["google_auth"].get("redirect_uri", "http://localhost:8501")

    # ── Handle OAuth callback ──────────────────────────────────────
    qp = st.query_params
    auth_code = qp.get("code")

    if auth_code:
        # Exchange authorization code for tokens (standard confidential-client flow, no PKCE)
        token_data = urllib.parse.urlencode({
            "code": auth_code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }).encode()

        try:
            req = urllib.request.Request(
                "https://oauth2.googleapis.com/token",
                data=token_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp = urllib.request.urlopen(req)
            tokens = json.loads(resp.read().decode())
        except urllib.error.HTTPError as he:
            err_body = he.read().decode()
            # If the code was already used (page refresh after login), show login button again
            if "invalid_grant" in err_body:
                st.query_params.clear()
                st.rerun()
            st.error(f"Token exchange failed: {err_body}")
            st.stop()
        except Exception as e:
            st.error(f"Token exchange failed: {e}")
            st.stop()

        # Decode ID token to get user info (JWT payload)
        id_token = tokens.get("id_token", "")
        try:
            payload = id_token.split(".")[1]
            # Fix base64 padding
            payload += "=" * (4 - len(payload) % 4)
            user_info = json.loads(base64.urlsafe_b64decode(payload))
        except Exception:
            st.error("Failed to decode user info from Google.")
            st.stop()

        email = user_info.get("email", "")
        name = user_info.get("name", email)

        # Clear the ?code= from URL
        st.query_params.clear()

        # Check authorization
        con = _get_auth_con()
        _init_allowed_users_table(con)

        # Ensure permanent admins always exist
        _PERM_ADMINS = [
            ("babuganesh2000@gmail.com", "Babu Ganesh"),
            ("sathishnb4u@gmail.com", "Sathish NB"),
        ]
        for pa_email, pa_name in _PERM_ADMINS:
            con.execute(
                "INSERT OR IGNORE INTO allowed_users (email, name, role) VALUES (?, ?, 'admin')",
                [pa_email, pa_name],
            )
            con.execute(
                "UPDATE allowed_users SET role = 'admin' WHERE LOWER(email) = LOWER(?) AND role != 'admin'",
                [pa_email],
            )

        if _is_user_allowed(con, email):
            st.session_state["authenticated"] = True
            st.session_state["user_email"] = email.lower()
            st.session_state["display_name"] = name
            st.session_state["user_role"] = _get_user_role(con, email)
            st.rerun()
        else:
            st.markdown(
                f"""
                <div style="text-align:center; padding:3rem 0">
                    <h1 style="color:#E20074">🚫 Access Denied</h1>
                    <p style="font-size:1.1em">Signed in as <b>{email}</b></p>
                    <p>Your email is not in the authorized users list.<br>
                    Please contact the admin to request access.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Sign out"):
                for k in ["authenticated", "user_email", "display_name", "user_role"]:
                    st.session_state.pop(k, None)
                st.rerun()
            st.stop()
    else:
        # ── Show login screen with Google sign-in button ───────────
        # Standard OAuth 2.0 auth code flow (no PKCE needed — we have client_secret)
        auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent",
        })

        st.markdown(
            f"""
            <div style="text-align:center; padding:4rem 0">
                <h1 style="color:#E20074">🔒 T-Mobile Bill Analytics</h1>
                <p style="font-size:1.1em; margin-bottom:2rem">Sign in with your Google account to continue</p>
                <a href="{auth_url}" style="
                    display:inline-block; padding:0.75rem 2rem;
                    background:#E20074; color:white; text-decoration:none;
                    border-radius:8px; font-size:1.1em; font-weight:600;
                ">🔑 Sign in with Google</a>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.stop()


def _get_auth_con():
    """Get a DuckDB connection for auth — MotherDuck or local file."""
    if "auth_con" not in st.session_state:
        if _is_motherduck():
            _ensure_md_databases()
            con = duckdb.connect(f"md:tmobile_auth?motherduck_token={_motherduck_token()}")
        else:
            con = duckdb.connect(AUTH_DB)
        _init_allowed_users_table(con)
        st.session_state["auth_con"] = con
    return st.session_state["auth_con"]


# ── Run login gate ──────────────────────────────────────────────────
login_gate()

# If auth is enabled but user is NOT authenticated, stop here
# (nothing — not even the sidebar — should render for unauthenticated visitors)
if _google_auth_enabled() and not st.session_state.get("authenticated"):
    st.stop()

# ─── Session-level DB ───────────────────────────────────────────────

def get_con() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection — MotherDuck (cloud) or local file."""
    if "db_con" not in st.session_state or st.session_state["db_con"] is None:
        if _is_motherduck():
            _ensure_md_databases()
            con = duckdb.connect(f"md:tmobile_bills?motherduck_token={_motherduck_token()}")
        else:
            os.makedirs(os.path.dirname(SEED_DB), exist_ok=True)
            con = duckdb.connect(SEED_DB)
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

# Show logged-in user & logout button
if _google_auth_enabled() and st.session_state.get("authenticated"):
    display = st.session_state.get("display_name", "")
    email = st.session_state.get("user_email", "")
    role = st.session_state.get("user_role", "viewer")
    st.sidebar.markdown(f"👤 **{display}**")
    st.sidebar.caption(f"{email} · {role}")
    if st.sidebar.button("Sign out", use_container_width=True):
        for key in ["authenticated", "user_email", "display_name", "user_role",
                     "connected", "user_info", "oauth_id"]:
            st.session_state.pop(key, None)
        st.rerun()
    st.sidebar.divider()

# ── Grouped sidebar navigation ────────────────────────────────────
_is_admin = st.session_state.get("user_role") == "admin"

# Define page groups — separators use "---" prefix so we can detect them
_SEPARATOR = "---"
_nav_items = []

# Core (high priority)
_nav_items.append(f"{_SEPARATOR}📋  CORE")
_nav_items += ["Upload Bills", "Overview", "Monthly Trends", "Bill Splitup"]

# Analytics (secondary)
_nav_items.append(f"{_SEPARATOR}📊  ANALYTICS")
_nav_items += ["Line Analysis", "Line Cost by Month", "Usage Details", "Person View"]

# Reports
_nav_items.append(f"{_SEPARATOR}📑  REPORTS")
_nav_items += ["Savings & Discounts", "Raw Data Explorer"]

# Admin (admin only)
if _is_admin:
    _nav_items.append(f"{_SEPARATOR}⚙️  ADMIN")
    _nav_items += ["User Management", "Phone Directory"]

# Render navigation — section headers are styled labels, pages are radio items
# We use a callback-driven approach with buttons for headers, radio for pages
def _render_nav():
    """Render grouped navigation in the sidebar."""
    selected = st.session_state.get("_nav_page", "Upload Bills")
    for item in _nav_items:
        if item.startswith(_SEPARATOR):
            # Section header
            label = item[len(_SEPARATOR):]
            st.sidebar.markdown(
                f"<div style='padding:8px 0 2px 0;font-size:0.75rem;font-weight:700;"
                f"color:#888;letter-spacing:0.05em;text-transform:uppercase'>"
                f"{label}</div>",
                unsafe_allow_html=True,
            )
        else:
            # Page button — highlight if selected
            is_active = (item == selected)
            btn_type = "primary" if is_active else "secondary"
            if st.sidebar.button(
                item,
                key=f"nav_{item}",
                use_container_width=True,
                type=btn_type,
            ):
                st.session_state["_nav_page"] = item
                st.rerun()

_render_nav()
page = st.session_state.get("_nav_page", "Upload Bills")


# ════════════════════════════════════════════════════════════════════
#  PAGE: Upload Bills
# ════════════════════════════════════════════════════════════════════

if page == "Upload Bills":
    st.title("Upload T-Mobile Bill PDFs")

    _is_admin = st.session_state.get("user_role") == "admin"

    if _is_admin:
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
                con.execute("CHECKPOINT")
                progress.empty()

                st.success(f"Processed {len(uploaded)} file(s)")
                st.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)
    else:
        st.info("Only **admin** users can upload bills. You have **viewer** access.")

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

        if _is_admin:
            st.divider()
            st.subheader("Reset Database")
            st.caption("Delete **all** loaded bills and start from scratch with an empty database.")
            if st.button("Delete All Data & Reset", type="secondary"):
                st.session_state["confirm_reset"] = True

            if st.session_state.get("confirm_reset"):
                st.warning("This will permanently delete every bill from the local database. Are you sure?")
                c1, c2 = st.columns(2)
                if c1.button("Yes, delete everything", type="primary"):
                    # Truncate only transaction / dimension tables; keep allowed_users intact
                    con = get_con()
                    _TRANSACTION_TABLES = [
                        "payments", "savings", "line_usage", "account_usage",
                        "line_charges", "invoices",
                        "person_lines", "persons", "lines",
                    ]
                    for tbl in _TRANSACTION_TABLES:
                        try:
                            con.execute(f"DELETE FROM {tbl}")
                        except Exception:
                            pass  # table may not exist yet
                    con.execute("CHECKPOINT")
                    st.session_state.pop("confirm_reset", None)
                    st.success("Database reset to empty. Upload new PDFs to begin.")
                    st.rerun()
                if c2.button("Cancel"):
                    st.session_state.pop("confirm_reset", None)
                    st.rerun()
    else:
        st.info("No bills loaded yet. Upload PDFs above to get started.")


# ─── Guard: remaining pages require data ────────────────────────────

_SKIP_DATA_PAGES = ("Upload Bills", "User Management", "Phone Directory")

if page not in _SKIP_DATA_PAGES and not has_data():
    st.warning("No data loaded. Go to **Upload Bills** to add PDFs first.")
    st.stop()

# ─── Load dataframes (only when data exists) ────────────────────────

if page not in _SKIP_DATA_PAGES and has_data():
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

    # Usage tables
    line_usage = run_query("SELECT * FROM line_usage_monthly ORDER BY bill_date, phone_number")
    usage_summary = run_query("SELECT * FROM usage_summary ORDER BY bill_date")
    account_usage = run_query("SELECT * FROM account_usage ORDER BY bill_date")

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
#  PAGE: Line Cost by Month
# ════════════════════════════════════════════════════════════════════

elif page == "Line Cost by Month":
    st.title("Line Cost by Month")

    # ── Separate line-level vs account-level charges ───────────────
    all_cost = line_charges.copy()
    all_cost["bill_date"] = pd.to_datetime(all_cost["bill_date"])
    all_cost["month"] = all_cost["bill_date"].apply(
        lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
    )
    all_months_sorted = invoices.sort_values("bill_date")["label"].tolist()

    line_data = all_cost[all_cost["phone_number"] != "Account"].copy()
    acct_data = all_cost[all_cost["phone_number"] == "Account"].copy()

    # Per-month: account charge & number of active lines
    acct_per_month = acct_data.groupby("month")["line_total"].sum()
    lines_per_month = line_data.groupby("month")["phone_number"].nunique()
    share_per_month = (acct_per_month / lines_per_month).fillna(0)  # per-line account share

    # ── Filters ────────────────────────────────────────────────────
    st.markdown("##### Filters")
    fc1, fc2, fc3 = st.columns([2, 2, 2])

    with fc1:
        preset = st.selectbox(
            "Time range",
            ["All months", "Last 3 months", "Last 6 months", "Last 12 months",
             "Year: 2024", "Year: 2025", "Year: 2026", "Custom range"],
            index=0,
        )

    if preset == "Last 3 months":
        cutoff_months = all_months_sorted[-3:]
    elif preset == "Last 6 months":
        cutoff_months = all_months_sorted[-6:]
    elif preset == "Last 12 months":
        cutoff_months = all_months_sorted[-12:]
    elif preset.startswith("Year:"):
        yr = preset.split(":")[1].strip()
        cutoff_months = [m for m in all_months_sorted if m.endswith(yr)]
    elif preset == "Custom range":
        with fc2:
            start_m = st.selectbox("From month", all_months_sorted, index=0)
        with fc3:
            end_m = st.selectbox("To month", all_months_sorted, index=len(all_months_sorted) - 1)
        si = all_months_sorted.index(start_m)
        ei = all_months_sorted.index(end_m)
        if si > ei:
            si, ei = ei, si
        cutoff_months = all_months_sorted[si:ei + 1]
    else:
        cutoff_months = all_months_sorted

    with fc2 if preset != "Custom range" else st.columns([1])[0]:
        line_types = ["All lines"] + sorted(lines_dim["line_type"].unique().tolist())
        sel_type = st.selectbox("Line type", line_types, index=0)

    # Apply filters
    filtered = line_data[line_data["month"].isin(cutoff_months)].copy()
    if sel_type != "All lines":
        type_phones = lines_dim[lines_dim["line_type"] == sel_type]["phone_number"].tolist()
        filtered = filtered[filtered["phone_number"].isin(type_phones)]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        st.stop()

    month_order = [m for m in all_months_sorted if m in cutoff_months]

    # ── Build line-charge pivot ────────────────────────────────────
    line_pivot = filtered.pivot_table(index="phone_number", columns="month",
                                       values="line_total", aggfunc="sum")
    line_pivot = line_pivot.reindex(columns=[m for m in month_order if m in line_pivot.columns])

    # ── Build account-share pivot (same shape, each cell = share) ──
    acct_share_pivot = line_pivot.copy()
    for m in acct_share_pivot.columns:
        # Only assign share where the line was active (has a charge)
        acct_share_pivot[m] = acct_share_pivot[m].apply(
            lambda v: round(share_per_month.get(m, 0), 2) if pd.notna(v) else None
        )

    # ── Combined pivot (line charge + account share) ───────────────
    combined_pivot = line_pivot.fillna(0) + acct_share_pivot.fillna(0)
    # Restore NaN where line wasn't active
    combined_pivot[line_pivot.isna()] = None

    # Add summary columns
    for pv, prefix in [(line_pivot, "Line"), (acct_share_pivot, "Acct Share"), (combined_pivot, "Total Due")]:
        pv[prefix + " TOTAL"] = pv[month_order].sum(axis=1, min_count=1)

    # Add TOTAL row to combined
    totals_row = combined_pivot.sum()
    totals_row.name = "TOTAL"
    combined_pivot_display = pd.concat([combined_pivot, totals_row.to_frame().T])

    n_months = len(month_order)
    n_lines = len(line_pivot)

    # ── KPIs ───────────────────────────────────────────────────────
    grand_total = combined_pivot_display.loc["TOTAL", "Total Due TOTAL"]
    acct_total_period = float(acct_share_pivot["Acct Share TOTAL"].sum())
    line_total_period = float(line_pivot["Line TOTAL"].sum())
    share_example = share_per_month.get(month_order[-1], 0) if month_order else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Grand Total", f"${grand_total:,.2f}")
    k2.metric("Line Charges", f"${line_total_period:,.2f}")
    k3.metric("Acct Share (all)", f"${acct_total_period:,.2f}")
    k4.metric("Acct Share/Line (latest)", f"${share_example:,.2f}/mo")

    st.caption(
        "💡 **Account-level charges** (~$81/mo for plan fees & taxes) are divided equally "
        f"among active lines each month. With {lines_per_month.get(month_order[-1], 0):.0f} lines "
        f"in the latest month, each line's share is **${share_example:,.2f}/mo**. "
        "The 'Total Due' column = Line Charge + Account Share."
    )

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Total Due (Line + Acct Share)", "Line Charges Only", "Heatmap", "Stacked Bar"
    ])

    with tab1:
        st.subheader("What Each Line Owes (incl. Account Share)")

        # Build a clean display: months | Line TOTAL | Acct Share TOTAL | Total Due TOTAL
        disp = combined_pivot_display.copy()
        disp.insert(len(month_order), "Line TOTAL", line_pivot["Line TOTAL"])
        disp.insert(len(month_order) + 1, "Acct Share", acct_share_pivot["Acct Share TOTAL"])
        # Fill TOTAL row for the summary columns
        disp.loc["TOTAL", "Line TOTAL"] = line_total_period
        disp.loc["TOTAL", "Acct Share"] = acct_total_period
        # Keep only month cols + 3 summary cols
        display_cols = [m for m in month_order if m in disp.columns] + ["Line TOTAL", "Acct Share", "Total Due TOTAL"]
        disp = disp[display_cols]

        def _style_total_due(s):
            if s.name == "TOTAL":
                return ["font-weight:bold; background-color:#E20074; color:white"] * len(s)
            return [""] * len(s)

        styled = disp.style.format("${:,.2f}", na_rep="-").apply(
            _style_total_due, axis=1
        ).map(
            lambda _: "font-weight:bold",
            subset=pd.IndexSlice[:, ["Line TOTAL", "Acct Share", "Total Due TOTAL"]]
        ).map(
            lambda _: "background-color:#E8F5E9",
            subset=pd.IndexSlice[:, "Total Due TOTAL"]
        ).map(
            lambda _: "background-color:#FFF3CD",
            subset=pd.IndexSlice[:, "Acct Share"]
        )
        tbl_height = min(750, 60 + 35 * (n_lines + 2))
        st.dataframe(styled, width="stretch", height=tbl_height)

    with tab2:
        st.subheader("Line Charges Only (excl. Account Share)")
        line_disp = line_pivot.copy()
        lt_row = line_disp.sum()
        lt_row.name = "TOTAL"
        line_disp = pd.concat([line_disp, lt_row.to_frame().T])

        def _style_line_only(s):
            if s.name == "TOTAL":
                return ["font-weight:bold; background-color:#E20074; color:white"] * len(s)
            return [""] * len(s)

        styled2 = line_disp.style.format("${:,.2f}", na_rep="-").apply(
            _style_line_only, axis=1
        ).map(
            lambda _: "font-weight:bold",
            subset=pd.IndexSlice[:, "Line TOTAL"]
        )
        tbl_height2 = min(750, 60 + 35 * (n_lines + 2))
        st.dataframe(styled2, width="stretch", height=tbl_height2)

    with tab3:
        st.subheader("Total Due Heatmap")
        heat = combined_pivot.drop(columns=["Total Due TOTAL"], errors="ignore")
        fig = px.imshow(
            heat.fillna(0).values,
            x=heat.columns.tolist(), y=heat.index.tolist(),
            color_continuous_scale=[[0, "#FFF0F6"], [0.5, "#E20074"], [1, "#8B004A"]],
            aspect="auto",
            title="Total Due Heatmap — Line Charge + Account Share (Phone × Month)",
            labels=dict(x="Month", y="Phone Number", color="$ Amount"),
        )
        fig.update_traces(text=heat.fillna(0).map(lambda v: f"${v:,.0f}").values,
                          texttemplate="%{text}", textfont_size=10)
        fig.update_layout(height=max(400, 45 * n_lines + 100))
        st.plotly_chart(fig, width="stretch")

    with tab4:
        st.subheader("Monthly Total Due by Line (Stacked)")
        # Build bar data from combined (line + share)
        bar_src = filtered.copy()
        bar_src["acct_share"] = bar_src["month"].map(share_per_month).fillna(0)
        bar_src["total_due"] = bar_src["line_total"] + bar_src["acct_share"]
        bar_data = bar_src.groupby(["month", "phone_number"])["total_due"].sum().reset_index()
        bar_data["month"] = pd.Categorical(bar_data["month"], categories=month_order, ordered=True)
        bar_data = bar_data.sort_values("month")
        fig = px.bar(
            bar_data, x="month", y="total_due", color="phone_number",
            title="Monthly Total Due by Line (incl. Account Share)",
            labels={"month": "Month", "total_due": "Total Due ($)", "phone_number": "Phone"},
            barmode="stack",
        )
        fig.update_layout(xaxis_tickangle=-45, height=550, legend=dict(font=dict(size=9)))
        st.plotly_chart(fig, width="stretch")


# ════════════════════════════════════════════════════════════════════
#  PAGE: Usage Details
# ════════════════════════════════════════════════════════════════════

elif page == "Usage Details":
    st.title("Usage Details")

    if line_usage.empty:
        st.warning("No usage data found.  Re-upload bills to extract usage details.")
    else:
        # ── KPI cards ──────────────────────────────────
        latest = usage_summary.iloc[-1] if not usage_summary.empty else None
        k1, k2, k3, k4 = st.columns(4)
        if latest is not None:
            k1.metric("Latest Talk (min)", f"{int(latest['total_talk_minutes']):,}")
            k2.metric("Latest Messages", f"{int(latest['total_messages']):,}")
            k3.metric("Latest Data (GB)", f"{latest['total_data_gb']:.1f}")
            k4.metric("Active Lines", f"{int(latest.get('voice_line_count', 0))}")

        tab1, tab2, tab3, tab4 = st.tabs(
            ["Data Usage", "Talk Usage", "Account Trends", "Per-Line Drill-down"]
        )

        # ── TAB 1: Data Usage ──────────────────────────
        with tab1:
            st.subheader("Data Usage by Line (GB)")
            data_pivot = line_usage.pivot_table(
                index="phone_number", columns="bill_month",
                values="data_gb", aggfunc="sum"
            )
            month_order = invoices["label"].tolist()
            data_pivot = data_pivot.reindex(
                columns=[m for m in month_order if m in data_pivot.columns]
            )

            fig_data = px.bar(
                line_usage, x="bill_month", y="data_gb", color="phone_number",
                title="Monthly Data Usage by Line",
                labels={"bill_month": "Month", "data_gb": "GB", "phone_number": "Phone"},
                category_orders={"bill_month": month_order},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_data.update_layout(barmode="stack", xaxis_tickangle=-45)
            st.plotly_chart(fig_data, use_container_width=True)

            # Top data users
            st.subheader("Top Data Users (All-Time)")
            top_data = (
                line_usage.groupby("phone_number")["data_gb"]
                .sum()
                .sort_values(ascending=True)
                .reset_index()
            )
            fig_top_data = px.bar(
                top_data, x="data_gb", y="phone_number", orientation="h",
                title="Total Data (GB) by Phone",
                labels={"data_gb": "Total GB", "phone_number": "Phone"},
                color_discrete_sequence=["#E20074"],
            )
            st.plotly_chart(fig_top_data, use_container_width=True)

            # Data heatmap
            st.subheader("Data Heatmap (Phone x Month)")
            fig_heat = px.imshow(
                data_pivot.fillna(0).values,
                x=data_pivot.columns.tolist(), y=data_pivot.index.tolist(),
                color_continuous_scale="Magenta", aspect="auto",
                labels=dict(x="Month", y="Phone", color="GB"),
            )
            fig_heat.update_layout(height=500)
            st.plotly_chart(fig_heat, use_container_width=True)

        # ── TAB 2: Talk Usage ──────────────────────────
        with tab2:
            st.subheader("Talk Minutes by Line")
            talk_pivot = line_usage.pivot_table(
                index="phone_number", columns="bill_month",
                values="talk_minutes", aggfunc="sum"
            )
            talk_pivot = talk_pivot.reindex(
                columns=[m for m in month_order if m in talk_pivot.columns]
            )

            fig_talk = px.bar(
                line_usage, x="bill_month", y="talk_minutes", color="phone_number",
                title="Monthly Talk Minutes by Line",
                labels={"bill_month": "Month", "talk_minutes": "Minutes", "phone_number": "Phone"},
                category_orders={"bill_month": month_order},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_talk.update_layout(barmode="stack", xaxis_tickangle=-45)
            st.plotly_chart(fig_talk, use_container_width=True)

            # Top talkers
            st.subheader("Top Talkers (All-Time)")
            top_talk = (
                line_usage.groupby("phone_number")["talk_minutes"]
                .sum()
                .sort_values(ascending=True)
                .reset_index()
            )
            fig_top_talk = px.bar(
                top_talk, x="talk_minutes", y="phone_number", orientation="h",
                title="Total Talk Minutes by Phone",
                labels={"talk_minutes": "Total Minutes", "phone_number": "Phone"},
                color_discrete_sequence=["#E20074"],
            )
            st.plotly_chart(fig_top_talk, use_container_width=True)

            # Talk heatmap
            st.subheader("Talk Heatmap (Phone x Month)")
            fig_theat = px.imshow(
                talk_pivot.fillna(0).values,
                x=talk_pivot.columns.tolist(), y=talk_pivot.index.tolist(),
                color_continuous_scale="Magenta", aspect="auto",
                labels=dict(x="Month", y="Phone", color="Minutes"),
            )
            fig_theat.update_layout(height=500)
            st.plotly_chart(fig_theat, use_container_width=True)

        # ── TAB 3: Account Trends ──────────────────────
        with tab3:
            st.subheader("Account-Level Usage Trends")
            if not usage_summary.empty:
                col1, col2 = st.columns(2)
                with col1:
                    fig_t = px.line(
                        usage_summary, x="bill_month", y="total_talk_minutes",
                        title="Total Talk Minutes per Month",
                        labels={"bill_month": "Month", "total_talk_minutes": "Minutes"},
                        markers=True,
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_t.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_t, use_container_width=True)

                with col2:
                    fig_d = px.line(
                        usage_summary, x="bill_month", y="total_data_gb",
                        title="Total Data (GB) per Month",
                        labels={"bill_month": "Month", "total_data_gb": "GB"},
                        markers=True,
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_d.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_d, use_container_width=True)

                col3, col4 = st.columns(2)
                with col3:
                    fig_m = px.line(
                        usage_summary, x="bill_month", y="total_messages",
                        title="Total Messages per Month",
                        labels={"bill_month": "Month", "total_messages": "Messages"},
                        markers=True,
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_m.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_m, use_container_width=True)

                with col4:
                    # Cost vs Data scatter
                    fig_cv = px.scatter(
                        usage_summary, x="total_data_gb", y="total_due",
                        title="Cost vs Data Usage",
                        labels={"total_data_gb": "Data (GB)", "total_due": "Total Due ($)"},
                        color_discrete_sequence=["#E20074"],
                    )
                    st.plotly_chart(fig_cv, use_container_width=True)

                # Usage per dollar
                st.subheader("Efficiency Metrics")
                eff = usage_summary.copy()
                eff["gb_per_dollar"] = eff["total_data_gb"] / eff["total_due"].replace(0, float("nan"))
                eff["min_per_dollar"] = eff["total_talk_minutes"] / eff["total_due"].replace(0, float("nan"))
                e1, e2 = st.columns(2)
                with e1:
                    fig_gbd = px.bar(
                        eff, x="bill_month", y="gb_per_dollar",
                        title="GB per Dollar",
                        labels={"bill_month": "Month", "gb_per_dollar": "GB/$"},
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_gbd.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_gbd, use_container_width=True)
                with e2:
                    fig_mpd = px.bar(
                        eff, x="bill_month", y="min_per_dollar",
                        title="Talk Minutes per Dollar",
                        labels={"bill_month": "Month", "min_per_dollar": "Min/$"},
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_mpd.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_mpd, use_container_width=True)
            else:
                st.info("No account-level usage data available.")

        # ── TAB 4: Per-Line Drill-down ─────────────────
        with tab4:
            st.subheader("Per-Line Usage History")
            phones = sorted(line_usage["phone_number"].unique())
            chosen = st.selectbox("Select Phone Number", phones)
            lu_phone = line_usage[line_usage["phone_number"] == chosen].sort_values("bill_date")

            if not lu_phone.empty:
                m1, m2, m3 = st.columns(3)
                m1.metric("Avg Talk (min/mo)", f"{lu_phone['talk_minutes'].mean():.0f}")
                m2.metric("Avg Data (GB/mo)", f"{lu_phone['data_gb'].mean():.1f}")
                total_cost = lu_phone["line_total"].sum() if "line_total" in lu_phone.columns else 0
                m3.metric("Total Charges", f"${total_cost:,.2f}")

                c1, c2 = st.columns(2)
                with c1:
                    fig_pt = px.bar(
                        lu_phone, x="bill_month", y="talk_minutes",
                        title=f"Talk Minutes – {chosen}",
                        labels={"bill_month": "Month", "talk_minutes": "Minutes"},
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_pt.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_pt, use_container_width=True)
                with c2:
                    fig_pd = px.bar(
                        lu_phone, x="bill_month", y="data_gb",
                        title=f"Data Usage – {chosen}",
                        labels={"bill_month": "Month", "data_gb": "GB"},
                        color_discrete_sequence=["#E20074"],
                    )
                    fig_pd.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_pd, use_container_width=True)

                # Usage vs cost for this line
                if "line_total" in lu_phone.columns:
                    fig_uc = px.scatter(
                        lu_phone, x="data_gb", y="line_total",
                        title=f"Data vs Cost – {chosen}",
                        labels={"data_gb": "Data (GB)", "line_total": "Charge ($)"},
                        color_discrete_sequence=["#E20074"],
                    )
                    st.plotly_chart(fig_uc, use_container_width=True)

                st.subheader("Raw Usage Data")
                st.dataframe(
                    lu_phone[["bill_month", "talk_minutes", "data_gb", "data_mb", "line_total"]].reset_index(drop=True),
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("No usage records for this phone number.")


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
#  PAGE: Bill Splitup
# ════════════════════════════════════════════════════════════════════

elif page == "Bill Splitup":
    st.title("Monthly Bill Splitup")

    month_labels = invoices.sort_values("bill_date")["label"].tolist()
    selected_label = st.selectbox(
        "Select Billing Month", month_labels, index=len(month_labels) - 1
    )

    inv_sel = invoices[invoices["label"] == selected_label].iloc[0]
    sel_date = inv_sel["bill_date"]

    # ── Invoice summary ────────────────────────────────────────────
    st.divider()
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Due", f"${float(inv_sel['total_due']):,.2f}")
    k2.metric("Plans", f"${float(inv_sel['plans_total']):,.2f}")
    k3.metric("Equipment", f"${float(inv_sel['equipment_total']):,.2f}")
    k4.metric("Savings", f"${float(inv_sel['total_savings']):,.2f}")
    k5.metric("Prev Balance", f"${float(inv_sel['previous_balance']):,.2f}")

    _fmt_d = lambda d: d.strftime("%b %d, %Y") if hasattr(d, "strftime") else str(d)
    st.caption(
        f"📅 Bill Date: **{_fmt_d(sel_date)}** · Due: **{_fmt_d(inv_sel['due_date'])}** · "
        f"Voice: **{inv_sel['voice_line_count']}** · "
        f"Devices: **{inv_sel['connected_device_count']}** · "
        f"Wearables: **{inv_sel['wearable_count']}**"
    )

    # ── Prepare data ───────────────────────────────────────────────
    month_lc = line_charges[line_charges["bill_date"] == sel_date].copy()
    acct_rows = month_lc[month_lc["phone_number"] == "Account"]
    indiv = month_lc[month_lc["phone_number"] != "Account"].copy()

    active = indiv[~indiv["status"].isin(["removed", "cancelled"])]
    acct_total = float(acct_rows["line_total"].sum()) if not acct_rows.empty else 0.0

    # Account share: Voice lines only (Mobile Internet & Wearable excluded)
    # Step 1: Lines with plans_charge < $10 get a $10 premium from account
    # Step 2: Remaining account charges split equally among ALL voice lines
    _PLAN_PREMIUM = 10.00
    voice_active = active[active["line_type"] == "Voice"]
    n_voice = len(voice_active)
    discounted_phones = set(
        voice_active[voice_active["plans_charge"] < _PLAN_PREMIUM]["phone_number"].tolist()
    )
    n_discounted = len(discounted_phones)
    premium_pool = _PLAN_PREMIUM * n_discounted
    remaining_pool = acct_total - premium_pool
    equal_share = round(remaining_pool / n_voice, 2) if n_voice > 0 else 0.0
    equal_remainder = round(remaining_pool - equal_share * n_voice, 2)
    n_active = len(active)

    name_map = (
        persons.drop_duplicates("phone_number")
        .set_index("phone_number")["person_label"]
        .to_dict()
    )
    indiv["Name"] = indiv["phone_number"].map(name_map).fillna(indiv["phone_number"])

    _type_ord = {"Voice": 0, "Mobile Internet": 1, "Wearable": 2}

    st.divider()
    tab1, tab2, tab3 = st.tabs([
        "📋 Bill Breakdown", "💰 Per-Person Splitup", "📊 Distribution"
    ])

    # ── TAB 1: Bill Breakdown ──────────────────────────────────────
    with tab1:
        st.subheader("T-Mobile Bill Breakdown")

        bd = indiv.copy()
        bd["_sort"] = bd["line_type"].map(_type_ord).fillna(3)
        bd = bd.sort_values(["_sort", "Name"]).drop(columns="_sort")
        bd = bd[["Name", "phone_number", "line_type", "status",
                  "plans_charge", "equipment_charge", "services_charge",
                  "onetime_charge", "line_total"]].copy()
        bd.columns = ["Name", "Phone", "Type", "Status",
                       "Plans", "Equipment", "Services", "One-Time", "Total"]

        acct_r = pd.DataFrame([{
            "Name": "Account", "Phone": "", "Type": "Account", "Status": "",
            "Plans": float(acct_rows["plans_charge"].sum()) if not acct_rows.empty else 0,
            "Equipment": float(acct_rows["equipment_charge"].sum()) if not acct_rows.empty else 0,
            "Services": float(acct_rows["services_charge"].sum()) if not acct_rows.empty else 0,
            "One-Time": float(acct_rows["onetime_charge"].sum()) if not acct_rows.empty else 0,
            "Total": acct_total,
        }])
        bd = pd.concat([bd, acct_r], ignore_index=True)

        t_row = pd.DataFrame([{
            "Name": "TOTAL", "Phone": "", "Type": "", "Status": "",
            "Plans": bd["Plans"].sum(), "Equipment": bd["Equipment"].sum(),
            "Services": bd["Services"].sum(), "One-Time": bd["One-Time"].sum(),
            "Total": bd["Total"].sum(),
        }])
        bd = pd.concat([bd, t_row], ignore_index=True)

        def _style_bd(row):
            if row["Name"] == "TOTAL":
                return ["font-weight:bold;background:#E20074;color:white"] * len(row)
            if row["Name"] == "Account":
                return ["font-weight:bold;background:#FFF3CD"] * len(row)
            if row["Status"] == "removed":
                return ["color:#999;font-style:italic"] * len(row)
            return [""] * len(row)

        st.dataframe(
            bd.style
            .format({"Plans": "${:,.2f}", "Equipment": "${:,.2f}",
                      "Services": "${:,.2f}", "One-Time": "${:,.2f}",
                      "Total": "${:,.2f}"})
            .apply(_style_bd, axis=1),
            use_container_width=True, hide_index=True,
            height=min(800, 60 + 35 * len(bd)),
        )

    # ── TAB 2: Per-Person Splitup (Voice only) ───────────────────
    with tab2:
        st.subheader("Per-Person Splitup")
        if n_discounted > 0:
            st.caption(
                f"Account **${acct_total:,.2f}** = "
                f"${_PLAN_PREMIUM:.0f} × {n_discounted} discounted line(s) "
                f"+ **${remaining_pool:,.2f}** ÷ {n_voice} voice lines "
                f"= **${equal_share:,.2f}**/line"
            )
        else:
            st.caption(
                f"Account charge **${acct_total:,.2f}** ÷ **{n_voice}** voice lines "
                f"= **${equal_share:,.2f}** per voice line"
            )

        # Only Voice lines participate in splitup
        sa = voice_active.copy()
        sa["Name"] = sa["phone_number"].map(name_map).fillna(sa["phone_number"])
        sa = sa.sort_values("Name").reset_index(drop=True)

        split_rows = []
        for idx in range(len(sa)):
            r = sa.iloc[idx]
            premium = _PLAN_PREMIUM if r["phone_number"] in discounted_phones else 0.0
            acct_sh = premium + equal_share
            # Add rounding remainder to last voice line
            if idx == len(sa) - 1:
                acct_sh += equal_remainder
            split_rows.append({
                "Name": r["Name"],
                "Phone": r["phone_number"],
                "Plan": float(r["plans_charge"]),
                "Services": float(r["services_charge"]),
                "Equipment": float(r["equipment_charge"]),
                "One-Time": float(r["onetime_charge"]),
                "Line Total": float(r["line_total"]),
                "Acct Share": round(acct_sh, 2),
                "Month Total": round(float(r["line_total"]) + acct_sh, 2),
            })

        sp = pd.DataFrame(split_rows)

        # MI / Wearable charges (excluded from split)
        non_voice = active[active["line_type"] != "Voice"]
        nv_total = float(non_voice["line_total"].sum()) if not non_voice.empty else 0.0

        t_sp = pd.DataFrame([{
            "Name": "TOTAL", "Phone": "",
            "Plan": sp["Plan"].sum(), "Services": sp["Services"].sum(),
            "Equipment": sp["Equipment"].sum(), "One-Time": sp["One-Time"].sum(),
            "Line Total": sp["Line Total"].sum(),
            "Acct Share": sp["Acct Share"].sum(),
            "Month Total": sp["Month Total"].sum(),
        }])
        sp_disp = pd.concat([sp, t_sp], ignore_index=True)

        money = {c: "${:,.2f}" for c in
                 ["Plan", "Services", "Equipment", "One-Time",
                  "Line Total", "Acct Share", "Month Total"]}

        def _style_sp(row):
            if row["Name"] == "TOTAL":
                return ["font-weight:bold;background:#E20074;color:white"] * len(row)
            styles = [""] * len(row)
            cols = row.index.tolist()
            for cn, bg in [("Acct Share", "#FFF3CD"), ("Month Total", "#E8F5E9")]:
                if cn in cols:
                    styles[cols.index(cn)] = f"background:{bg};font-weight:bold;"
            return styles

        st.dataframe(
            sp_disp.style.format(money).apply(_style_sp, axis=1),
            use_container_width=True, hide_index=True,
            height=min(800, 60 + 35 * len(sp_disp)),
        )

        # Validation against invoice
        voice_split_total = float(sp_disp.iloc[-1]["Month Total"])
        inv_total = float(inv_sel["total_due"])
        grand_total = voice_split_total + nv_total

        if nv_total > 0:
            st.info(
                f"📱 Mobile Internet / Wearable charges: **${nv_total:,.2f}** "
                f"(not included in per-person split)"
            )

        if abs(grand_total - inv_total) < 0.02:
            st.success(
                f"✅ Voice split **${voice_split_total:,.2f}** + "
                f"MI/Wearable **${nv_total:,.2f}** = "
                f"**${grand_total:,.2f}** matches invoice **${inv_total:,.2f}**"
            )
        else:
            st.warning(
                f"⚠️ Voice split **${voice_split_total:,.2f}** + "
                f"MI/Wearable **${nv_total:,.2f}** = **${grand_total:,.2f}** "
                f"vs invoice **${inv_total:,.2f}** "
                f"(diff: ${grand_total - inv_total:+,.2f} — removed/cancelled lines excluded)"
            )

    # ── TAB 3: Distribution ────────────────────────────────────────
    with tab3:
        st.subheader("Cost Distribution")
        col1, col2 = st.columns(2)

        with col1:
            if not sp.empty:
                fig_pie1 = px.pie(
                    sp, names="Name", values="Month Total",
                    title=f"Voice Line Cost Share — {selected_label}",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.3,
                )
                fig_pie1.update_traces(
                    textinfo="label+value+percent",
                    texttemplate="%{label}<br>$%{value:,.2f}<br>(%{percent})",
                )
                st.plotly_chart(fig_pie1, use_container_width=True)

        with col2:
            cats = {
                "Plans": float(active["plans_charge"].sum()),
                "Equipment": float(active["equipment_charge"].sum()),
                "Services": float(active["services_charge"].sum()),
                "One-Time": float(active["onetime_charge"].sum()),
                "Account": acct_total,
            }
            cats = {k: v for k, v in cats.items() if v != 0}
            if cats:
                fig_pie2 = px.pie(
                    names=list(cats.keys()), values=list(cats.values()),
                    title=f"Charge Category Mix — {selected_label}",
                    color_discrete_sequence=["#E20074", "#5C2D91", "#0078D4",
                                              "#FFB900", "#107C10"],
                    hole=0.3,
                )
                fig_pie2.update_traces(
                    textinfo="label+value+percent",
                    texttemplate="%{label}<br>$%{value:,.2f}<br>(%{percent})",
                )
                st.plotly_chart(fig_pie2, use_container_width=True)

        bar_df = sp.sort_values("Month Total", ascending=True)
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            y=bar_df["Name"], x=bar_df["Line Total"],
            name="Line Charges", orientation="h", marker_color="#E20074",
        ))
        fig_bar.add_trace(go.Bar(
            y=bar_df["Name"], x=bar_df["Acct Share"],
            name="Account Share", orientation="h", marker_color="#FFB900",
        ))
        fig_bar.update_layout(
            barmode="stack",
            title=f"Per-Person: Line Charges + Account Share — {selected_label}",
            xaxis_title="Amount ($)", yaxis_title="",
            height=max(400, 40 * len(bar_df) + 100),
        )
        st.plotly_chart(fig_bar, use_container_width=True)


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
        "line_usage": "Per-phone talk/data usage per bill",
        "account_usage": "Account-level usage totals per bill",
        "person_monthly_cost": "View: charges joined to persons by month",
        "person_total": "View: aggregated per-person totals",
        "line_usage_monthly": "View: per-line usage with bill month & charges",
        "usage_summary": "View: account-level usage with bill totals",
        "phone_names": "Phone number to person name mappings",
    }
    for tbl, desc in tables_info.items():
        st.code(f"{tbl}  --  {desc}", language=None)


# ════════════════════════════════════════════════════════════════════
#  PAGE: User Management (admin only)
# ════════════════════════════════════════════════════════════════════

elif page == "User Management":
    st.title("User Management")

    if st.session_state.get("user_role") != "admin":
        st.error("Admin access required.")
        st.stop()

    # Permanent admins — cannot be removed or demoted
    PERMANENT_ADMINS = {"babuganesh2000@gmail.com", "sathishnb4u@gmail.com"}

    auth_con = _get_auth_con()
    _init_allowed_users_table(auth_con)

    # ── Current Users ──────────────────────────────────────────────
    st.subheader("Authorized Users")
    users_df = auth_con.execute(
        "SELECT email, name, role, added_date FROM allowed_users ORDER BY added_date"
    ).fetchdf()

    if users_df.empty:
        st.info("No users in the list yet.")
    else:
        # Mark permanent admins visually
        display_df = users_df.copy()
        display_df["protected"] = display_df["email"].apply(
            lambda e: "🔒 Permanent" if e.lower() in PERMANENT_ADMINS else ""
        )
        st.dataframe(display_df, width="stretch", hide_index=True)

    st.divider()

    # ── Add User ───────────────────────────────────────────────────
    st.subheader("Add User")
    st.caption("New users are always added as **viewer** (read-only). Only permanent admins have admin access.")
    with st.form("add_user_form", clear_on_submit=True):
        col1, col2 = st.columns([2, 2])
        new_email = col1.text_input("Gmail / Google email", placeholder="user@gmail.com")
        new_name = col2.text_input("Display name", placeholder="John Doe")
        submitted = st.form_submit_button("Add User", type="primary")

        if submitted:
            if not new_email or "@" not in new_email:
                st.error("Enter a valid email address.")
            else:
                try:
                    auth_con.execute(
                        "INSERT INTO allowed_users (email, name, role) VALUES (?, ?, 'viewer')",
                        [new_email.lower().strip(), new_name.strip()],
                    )
                    st.success(f"✅ Added **{new_email}** as **viewer**")
                    st.rerun()
                except duckdb.ConstraintException:
                    st.warning(f"**{new_email}** is already in the list.")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()

    # ── Remove User ────────────────────────────────────────────────
    st.subheader("Remove User")
    if not users_df.empty:
        # Exclude permanent admins and the current user from removal
        removable = users_df[
            ~users_df["email"].str.lower().isin(PERMANENT_ADMINS)
            & (users_df["email"] != st.session_state.get("user_email", ""))
        ]["email"].tolist()
        if removable:
            del_email = st.selectbox("Select user to remove", removable)
            if st.button("Remove User", type="secondary"):
                auth_con.execute(
                    "DELETE FROM allowed_users WHERE LOWER(email) = LOWER(?)",
                    [del_email],
                )
                st.success(f"Removed **{del_email}**")
                st.rerun()
        else:
            st.info("No removable users. Permanent admins cannot be removed.")


# ════════════════════════════════════════════════════════════════════
#  PAGE: Phone Directory (admin only)
# ════════════════════════════════════════════════════════════════════

elif page == "Phone Directory":
    st.title("📞 Phone Directory")

    if st.session_state.get("user_role") != "admin":
        st.error("Admin access required.")
        st.stop()

    st.caption(
        "Assign names to phone numbers. These names appear in **Bill Splitup**, "
        "**Person View**, and all person-related charts. Changes take effect after "
        "clicking **Save & Rebuild**."
    )

    bills_con = get_con()

    # Ensure phone_names table exists
    bills_con.execute("""
        CREATE TABLE IF NOT EXISTS phone_names (
            phone_number  VARCHAR PRIMARY KEY,
            person_name   VARCHAR NOT NULL
        )
    """)

    # Load all known phones from lines dimension
    all_phones_df = bills_con.execute("""
        SELECT l.phone_number, l.line_type, l.is_current, l.lifecycle_notes,
               pn.person_name
        FROM lines l
        LEFT JOIN phone_names pn ON l.phone_number = pn.phone_number
        ORDER BY l.line_type, l.phone_number
    """).fetchdf()

    if all_phones_df.empty:
        st.info("No phone lines found. Upload bills first.")
    else:
        # Show current directory
        st.subheader("Current Assignments")
        dir_display = all_phones_df.copy()
        dir_display["person_name"] = dir_display["person_name"].fillna("")
        dir_display["status"] = dir_display.apply(
            lambda r: "✅ Active" if r["is_current"] else f"❌ {r['lifecycle_notes']}", axis=1
        )
        dir_display = dir_display[["phone_number", "person_name", "line_type", "status"]]
        dir_display.columns = ["Phone", "Name", "Type", "Status"]

        def _style_dir(row):
            if not row["Name"]:
                return ["background:#FFF3CD"] * len(row)
            return [""] * len(row)
        st.dataframe(
            dir_display.style.apply(_style_dir, axis=1),
            use_container_width=True, hide_index=True,
            height=min(600, 60 + 35 * len(dir_display)),
        )
        unnamed_ct = (dir_display["Name"] == "").sum()
        if unnamed_ct:
            st.caption(f"⚠️ **{unnamed_ct}** phone(s) without a name (highlighted yellow)")

        st.divider()

        # Edit form — one row per phone
        st.subheader("Edit Names")
        with st.form("phone_dir_form", clear_on_submit=False):
            edited_names = {}
            cols_per_row = 3
            phones = all_phones_df["phone_number"].tolist()
            current_names = all_phones_df["person_name"].fillna("").tolist()
            line_types = all_phones_df["line_type"].tolist()

            for i in range(0, len(phones), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j, col in enumerate(row_cols):
                    idx = i + j
                    if idx < len(phones):
                        phone = phones[idx]
                        cur = current_names[idx]
                        ltype = line_types[idx]
                        icon = "📱" if ltype == "Voice" else ("📡" if ltype == "Mobile Internet" else "⌚")
                        edited_names[phone] = col.text_input(
                            f"{icon} {phone}",
                            value=cur,
                            key=f"pn_{phone}",
                            placeholder="Enter name",
                        )

            save_btn = st.form_submit_button("💾 Save & Rebuild", type="primary")

        if save_btn:
            changes = 0
            for phone, name in edited_names.items():
                name = name.strip()
                if name:
                    bills_con.execute(
                        "INSERT OR REPLACE INTO phone_names VALUES (?,?)",
                        [phone, name],
                    )
                    changes += 1
                else:
                    # Remove name if cleared
                    bills_con.execute(
                        "DELETE FROM phone_names WHERE phone_number = ?",
                        [phone],
                    )

            # Rebuild persons with new names
            rebuild_dimensions(bills_con)
            bills_con.execute("CHECKPOINT")
            st.success(f"✅ Saved {changes} name(s) and rebuilt person mapping.")
            st.rerun()

