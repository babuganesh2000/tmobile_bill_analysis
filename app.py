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
_nav_items += ["Line Analysis", "Line Cost by Month", "Person View"]

# Reports
_nav_items.append(f"{_SEPARATOR}📑  REPORTS")
_nav_items += ["Savings & Discounts", "Raw Data Explorer"]

# Admin (admin only)
if _is_admin:
    _nav_items.append(f"{_SEPARATOR}⚙️  ADMIN")
    _nav_items += ["Balances", "User Management", "Phone Directory"]

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
                        "payments", "savings",
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

    invoices["label"] = invoices["bill_date"].apply(
        lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
    )
    num_months = len(invoices)

    # ── Universal account-share helper ─────────────────────────────
    _PLAN_PREMIUM = 10.00

    def _compute_acct_shares(lc_month: pd.DataFrame) -> dict:
        """Return {phone_number: acct_share} for one billing month.

        Rules (same as Bill Splitup):
        - Account share goes to Voice lines only; MI & Wearable get $0.
        - Discounted voice lines (plans_charge < $10) pay a $10 premium
          from the account pool first.
        - Remaining account pool is split equally among all voice lines.
        """
        acct_rows = lc_month[lc_month["phone_number"] == "Account"]
        indiv = lc_month[lc_month["phone_number"] != "Account"]
        active = indiv[~indiv["status"].isin(["removed", "cancelled"])]
        acct_total = float(acct_rows["line_total"].sum()) if not acct_rows.empty else 0.0

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

        shares = {}
        voice_idx = 0
        for phone in active["phone_number"].tolist():
            lt = active.loc[active["phone_number"] == phone, "line_type"].iloc[0]
            if lt == "Voice":
                voice_idx += 1
                premium = _PLAN_PREMIUM if phone in discounted_phones else 0.0
                sh = premium + equal_share
                if voice_idx == n_voice:
                    sh += equal_remainder
                shares[phone] = round(sh, 2)
            else:
                shares[phone] = 0.0
        return shares

    def _build_month_splitup(lc_month: pd.DataFrame) -> pd.DataFrame:
        """Build per-line splitup for one month with account share + taxes."""
        shares = _compute_acct_shares(lc_month)
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
                "status": r["status"],
                "plan": plan, "equipment": equip, "services": svc,
                "onetime": ot, "taxes_fees": taxes,
                "line_total": total, "acct_share": acct_sh,
                "month_total": round(total + acct_sh, 2),
            })
        return pd.DataFrame(rows)


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
         "onetime_charges", "total_savings"],
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

        melt_src = ph_data[["label", "plans_charge", "equipment_charge", "services_charge", "onetime_charge", "line_total"]].copy()
        melt_src["taxes_fees"] = (melt_src["line_total"] - melt_src["plans_charge"]
                                   - melt_src["equipment_charge"] - melt_src["services_charge"]
                                   - melt_src["onetime_charge"]).round(2)
        melt = melt_src[["label", "plans_charge", "equipment_charge", "services_charge", "onetime_charge", "taxes_fees"]].melt(
            id_vars="label", var_name="Category", value_name="Amount"
        )
        melt["Category"] = melt["Category"].map({
            "plans_charge": "Plan", "equipment_charge": "Equipment",
            "services_charge": "Services", "onetime_charge": "One-Time",
            "taxes_fees": "Taxes & Fees",
        })
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

    # ── Prepare data ───────────────────────────────────────────────
    all_cost = line_charges.copy()
    all_cost["bill_date"] = pd.to_datetime(all_cost["bill_date"])
    all_cost["month"] = all_cost["bill_date"].apply(
        lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d)[:7]
    )
    all_months_sorted = invoices.sort_values("bill_date")["label"].tolist()

    line_data = all_cost[all_cost["phone_number"] != "Account"].copy()

    # ── Build per-line per-month splitup using universal helper ────
    splitup_rows = []
    for bill_date in line_charges["bill_date"].unique():
        lc_m = line_charges[line_charges["bill_date"] == bill_date]
        sp = _build_month_splitup(lc_m)
        month_label = pd.Timestamp(bill_date).strftime("%b %Y")
        sp["month"] = month_label
        sp["bill_date"] = bill_date
        splitup_rows.append(sp)
    all_splitup = pd.concat(splitup_rows, ignore_index=True) if splitup_rows else pd.DataFrame()

    # Name map
    _name_map = (
        persons.drop_duplicates("phone_number")
        .set_index("phone_number")["person_label"].to_dict()
    )
    if not all_splitup.empty:
        all_splitup["name"] = all_splitup["phone_number"].map(_name_map).fillna(all_splitup["phone_number"])

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
    filtered = all_splitup[all_splitup["month"].isin(cutoff_months)].copy()
    if sel_type != "All lines":
        filtered = filtered[filtered["line_type"] == sel_type]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        st.stop()

    month_order = [m for m in all_months_sorted if m in cutoff_months]

    # ── Build pivots ───────────────────────────────────────────────
    # Month Total pivot (line charge + taxes + acct share)
    mt_pivot = filtered.pivot_table(index="phone_number", columns="month",
                                     values="month_total", aggfunc="sum")
    mt_pivot = mt_pivot.reindex(columns=[m for m in month_order if m in mt_pivot.columns])

    # Line charge pivot (plan + equip + svc + onetime + taxes, no acct share)
    lt_pivot = filtered.pivot_table(index="phone_number", columns="month",
                                     values="line_total", aggfunc="sum")
    lt_pivot = lt_pivot.reindex(columns=[m for m in month_order if m in lt_pivot.columns])

    # Taxes pivot
    tax_pivot = filtered.pivot_table(index="phone_number", columns="month",
                                      values="taxes_fees", aggfunc="sum")
    tax_pivot = tax_pivot.reindex(columns=[m for m in month_order if m in tax_pivot.columns])

    # Acct share pivot
    as_pivot = filtered.pivot_table(index="phone_number", columns="month",
                                     values="acct_share", aggfunc="sum")
    as_pivot = as_pivot.reindex(columns=[m for m in month_order if m in as_pivot.columns])

    # Summary columns
    mt_pivot["Month Total TOTAL"] = mt_pivot[[m for m in month_order if m in mt_pivot.columns]].sum(axis=1, min_count=1)
    lt_pivot["Line TOTAL"] = lt_pivot[[m for m in month_order if m in lt_pivot.columns]].sum(axis=1, min_count=1)
    tax_pivot["Taxes TOTAL"] = tax_pivot[[m for m in month_order if m in tax_pivot.columns]].sum(axis=1, min_count=1)
    as_pivot["Acct Share TOTAL"] = as_pivot[[m for m in month_order if m in as_pivot.columns]].sum(axis=1, min_count=1)

    n_months = len(month_order)
    n_lines = len(mt_pivot)

    # ── KPIs ───────────────────────────────────────────────────────
    grand_total = float(mt_pivot["Month Total TOTAL"].sum())
    line_total_period = float(lt_pivot["Line TOTAL"].sum())
    taxes_total_period = float(tax_pivot["Taxes TOTAL"].sum())
    acct_total_period = float(as_pivot["Acct Share TOTAL"].sum())

    # Latest month account share per voice line
    latest_month = month_order[-1] if month_order else None
    if latest_month:
        _latest_shares = filtered[filtered["month"] == latest_month]
        _voice_shares = _latest_shares[_latest_shares["line_type"] == "Voice"]["acct_share"]
        share_example = float(_voice_shares.iloc[0]) if len(_voice_shares) > 0 else 0.0
        n_voice_latest = len(_voice_shares)
    else:
        share_example = 0.0
        n_voice_latest = 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Grand Total", f"${grand_total:,.2f}")
    k2.metric("Line Charges", f"${line_total_period:,.2f}")
    k3.metric("Taxes & Fees", f"${taxes_total_period:,.2f}")
    k4.metric("Acct Share (all)", f"${acct_total_period:,.2f}")

    st.caption(
        "💡 **Account share** goes to **Voice lines only**. "
        "Discounted voice lines (plan < $10) pay a $10 premium first; "
        f"remaining account charges split equally among voice lines. "
        f"MI & Wearable: **$0** account share. "
        f"Latest month: **${share_example:,.2f}**/voice line "
        f"across {n_voice_latest} voice lines. "
        "**Taxes & Fees** = Total − Plan − Equipment − Services − One-Time."
    )

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Month Total (Line + Acct Share)", "Breakdown", "Heatmap", "Stacked Bar"
    ])

    def _add_total_row(pv):
        tr = pv.sum()
        tr.name = "TOTAL"
        return pd.concat([pv, tr.to_frame().T])

    def _style_bold_total(s):
        if s.name == "TOTAL":
            return ["font-weight:bold; background-color:#E20074; color:white"] * len(s)
        return [""] * len(s)

    tbl_height = min(750, 60 + 35 * (n_lines + 2))

    with tab1:
        st.subheader("What Each Line Owes (incl. Account Share)")
        disp = mt_pivot.copy()
        disp["Line Total"] = lt_pivot["Line TOTAL"]
        disp["Taxes & Fees"] = tax_pivot["Taxes TOTAL"]
        disp["Acct Share"] = as_pivot["Acct Share TOTAL"]
        disp = _add_total_row(disp)
        display_cols = [m for m in month_order if m in disp.columns] + \
                       ["Line Total", "Taxes & Fees", "Acct Share", "Month Total TOTAL"]
        disp = disp[display_cols]

        styled = disp.style.format("${:,.2f}", na_rep="-").apply(
            _style_bold_total, axis=1
        ).map(
            lambda _: "font-weight:bold",
            subset=pd.IndexSlice[:, ["Line Total", "Taxes & Fees", "Acct Share", "Month Total TOTAL"]]
        ).map(
            lambda _: "background-color:#E8F5E9",
            subset=pd.IndexSlice[:, "Month Total TOTAL"]
        ).map(
            lambda _: "background-color:#FFF3CD",
            subset=pd.IndexSlice[:, "Acct Share"]
        ).map(
            lambda _: "background-color:#FDE8E8",
            subset=pd.IndexSlice[:, "Taxes & Fees"]
        )
        st.dataframe(styled, width="stretch", height=tbl_height)

    with tab2:
        st.subheader("Charge Breakdown by Line")
        bk = filtered.groupby("phone_number").agg(
            Plan=("plan", "sum"), Equipment=("equipment", "sum"),
            Services=("services", "sum"), **{"One-Time": ("onetime", "sum")},
            **{"Taxes & Fees": ("taxes_fees", "sum")},
            **{"Line Total": ("line_total", "sum")},
            **{"Acct Share": ("acct_share", "sum")},
            **{"Month Total": ("month_total", "sum")},
        ).reset_index()
        bk["Name"] = bk["phone_number"].map(_name_map).fillna(bk["phone_number"])
        bk["Type"] = bk["phone_number"].map(
            filtered.drop_duplicates("phone_number").set_index("phone_number")["line_type"]
        )
        bk = bk[["Name", "phone_number", "Type", "Plan", "Equipment", "Services",
                  "One-Time", "Taxes & Fees", "Line Total", "Acct Share", "Month Total"]]
        bk = bk.sort_values("Month Total", ascending=False)

        # TOTAL row
        totals = {c: bk[c].sum() for c in bk.columns if c not in ("Name", "phone_number", "Type")}
        totals.update({"Name": "TOTAL", "phone_number": "", "Type": ""})
        bk = pd.concat([bk, pd.DataFrame([totals])], ignore_index=True)

        def _style_bk(s):
            if s.get("Name") == "TOTAL":
                return ["font-weight:bold; background-color:#E20074; color:white"] * len(s)
            return [""] * len(s)

        fmt = {c: "${:,.2f}" for c in bk.columns if c not in ("Name", "phone_number", "Type")}
        styled_bk = bk.style.format(fmt, na_rep="-").apply(_style_bk, axis=1)
        st.dataframe(styled_bk, width="stretch", height=tbl_height, hide_index=True)

    with tab3:
        st.subheader("Month Total Heatmap")
        heat = mt_pivot.drop(columns=["Month Total TOTAL"], errors="ignore")
        fig = px.imshow(
            heat.fillna(0).values,
            x=heat.columns.tolist(), y=heat.index.tolist(),
            color_continuous_scale=[[0, "#FFF0F6"], [0.5, "#E20074"], [1, "#8B004A"]],
            aspect="auto",
            title="Month Total Heatmap — Line Charge + Acct Share (Phone × Month)",
            labels=dict(x="Month", y="Phone Number", color="$ Amount"),
        )
        fig.update_traces(text=heat.fillna(0).map(lambda v: f"${v:,.0f}").values,
                          texttemplate="%{text}", textfont_size=10)
        fig.update_layout(height=max(400, 45 * n_lines + 100))
        st.plotly_chart(fig, width="stretch")

    with tab4:
        st.subheader("Monthly Total by Line (Stacked)")
        bar_data = filtered.groupby(["month", "phone_number"])["month_total"].sum().reset_index()
        bar_data["month"] = pd.Categorical(bar_data["month"], categories=month_order, ordered=True)
        bar_data = bar_data.sort_values("month")
        fig = px.bar(
            bar_data, x="month", y="month_total", color="phone_number",
            title="Monthly Total Due by Line (incl. Taxes & Acct Share)",
            labels={"month": "Month", "month_total": "Month Total ($)", "phone_number": "Phone"},
            barmode="stack",
        )
        fig.update_layout(xaxis_tickangle=-45, height=550, legend=dict(font=dict(size=9)))
        st.plotly_chart(fig, width="stretch")


# ════════════════════════════════════════════════════════════════════
#  PAGE: Person View
# ════════════════════════════════════════════════════════════════════

elif page == "Person View":
    st.title("Person-Level Analysis")

    # ── Build full splitup with account share across all months ────
    _name_map_pv = (
        persons.drop_duplicates("phone_number")
        .set_index("phone_number")["person_label"].to_dict()
    )
    _phone_person = {}  # phone -> person_label (respecting effective dates)
    for _, pr in persons.iterrows():
        _phone_person[pr["phone_number"]] = pr["person_label"]

    pv_rows = []
    for bill_date in line_charges["bill_date"].unique():
        lc_m = line_charges[line_charges["bill_date"] == bill_date]
        sp = _build_month_splitup(lc_m)
        sp["bill_date"] = bill_date
        sp["month"] = pd.Timestamp(bill_date).strftime("%b %Y")
        pv_rows.append(sp)
    pv_all = pd.concat(pv_rows, ignore_index=True) if pv_rows else pd.DataFrame()

    if not pv_all.empty:
        # Map phones to persons via person_monthly (which respects effective dates)
        pm_phone_person = person_monthly[["phone_number", "bill_date", "person_label"]].drop_duplicates()
        pv_all = pv_all.merge(pm_phone_person, on=["phone_number", "bill_date"], how="left")
        pv_all["person_label"] = pv_all["person_label"].fillna(
            pv_all["phone_number"].map(_name_map_pv)
        ).fillna(pv_all["phone_number"])

    tab1, tab2 = st.tabs(["Totals", "Monthly Drill-down"])

    with tab1:
        st.subheader("Total Cost per Person (All Months)")

        if not pv_all.empty:
            pt = pv_all.groupby("person_label").agg(
                total_plan=("plan", "sum"),
                total_equipment=("equipment", "sum"),
                total_services=("services", "sum"),
                total_onetime=("onetime", "sum"),
                total_taxes=("taxes_fees", "sum"),
                total_line=("line_total", "sum"),
                total_acct_share=("acct_share", "sum"),
                grand_total=("month_total", "sum"),
                months=("month", "nunique"),
            ).reset_index().sort_values("grand_total", ascending=False)

            fig = px.bar(
                pt.sort_values("grand_total", ascending=True),
                x="grand_total", y="person_label", orientation="h",
                color="grand_total", color_continuous_scale=["#FFB900", "#E20074"],
                title="Total Cost by Person (incl. Acct Share)",
                labels={"grand_total": "Total ($)", "person_label": "Person"},
            )
            fig.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig, width='stretch')

            # Display table
            pt_disp = pt.rename(columns={
                "person_label": "Person", "total_plan": "Plan",
                "total_equipment": "Equipment", "total_services": "Services",
                "total_onetime": "One-Time", "total_taxes": "Taxes & Fees",
                "total_line": "Line Total", "total_acct_share": "Acct Share",
                "grand_total": "Grand Total", "months": "Months",
            })
            pt_disp = pt_disp[["Person", "Months", "Plan", "Equipment", "Services",
                               "One-Time", "Taxes & Fees", "Line Total", "Acct Share", "Grand Total"]]
            st.dataframe(pt_disp, width='stretch', hide_index=True)
        else:
            st.warning("No data available.")

        st.subheader("Person - Phone Mapping")
        st.dataframe(persons, width='stretch', hide_index=True)

    with tab2:
        st.subheader("Monthly Cost per Person")
        person_list = sorted(pv_all["person_label"].unique().tolist()) if not pv_all.empty else []
        sel_person = st.selectbox("Select person", person_list)

        p_data = pv_all[pv_all["person_label"] == sel_person].copy() if sel_person else pd.DataFrame()

        if not p_data.empty:
            month_order_pv = invoices.sort_values("bill_date")["label"].tolist()
            monthly_agg = p_data.groupby("month", sort=False).agg(
                plan=("plan", "sum"), equipment=("equipment", "sum"),
                services=("services", "sum"), onetime=("onetime", "sum"),
                taxes_fees=("taxes_fees", "sum"), line_total=("line_total", "sum"),
                acct_share=("acct_share", "sum"), month_total=("month_total", "sum"),
            ).reset_index()
            monthly_agg["month"] = pd.Categorical(
                monthly_agg["month"],
                categories=[m for m in month_order_pv if m in monthly_agg["month"].values],
                ordered=True,
            )
            monthly_agg = monthly_agg.sort_values("month")

            # Bar chart showing month_total (includes acct share)
            fig = px.bar(
                monthly_agg, x="month", y="month_total",
                color_discrete_sequence=["#E20074"],
                title=f"Monthly Total - {sel_person} (incl. Acct Share)",
                labels={"month": "Month", "month_total": "Month Total ($)"},
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, width='stretch')

            # Pie chart with all categories including acct share
            _plan_sum = float(p_data["plan"].sum())
            _equip_sum = float(p_data["equipment"].sum())
            _svc_sum = float(p_data["services"].sum())
            _ot_sum = float(p_data["onetime"].sum())
            _tax_sum = float(p_data["taxes_fees"].sum())
            _acct_sum = float(p_data["acct_share"].sum())
            cats = {
                "Plan": _plan_sum,
                "Equipment": _equip_sum,
                "Services": _svc_sum,
                "One-Time": _ot_sum,
                "Taxes & Fees": _tax_sum,
                "Acct Share": _acct_sum,
            }
            cats = {k: v for k, v in cats.items() if v != 0}
            if cats:
                fig2 = px.pie(
                    names=list(cats.keys()), values=list(cats.values()),
                    title=f"Charge Mix - {sel_person} (incl. Acct Share)",
                    color_discrete_sequence=["#E20074", "#5C2D91", "#0078D4", "#FFB900", "#107C10", "#FF6F61"],
                )
                st.plotly_chart(fig2, width='stretch')

            # Detail table
            detail_cols = {
                "month": "Month", "phone_number": "Phone", "line_type": "Type",
                "plan": "Plan", "equipment": "Equipment", "services": "Services",
                "onetime": "One-Time", "taxes_fees": "Taxes & Fees",
                "line_total": "Line Total", "acct_share": "Acct Share",
                "month_total": "Month Total",
            }
            p_disp = p_data[list(detail_cols.keys())].rename(columns=detail_cols)
            p_disp["Month"] = pd.Categorical(
                p_disp["Month"],
                categories=[m for m in month_order_pv if m in p_disp["Month"].values],
                ordered=True,
            )
            p_disp = p_disp.sort_values(["Month", "Phone"])
            st.dataframe(p_disp, width='stretch', hide_index=True)
        else:
            st.info("No data for selected person.")


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
                       "Plan", "Equipment", "Services", "One-Time", "Total"]
        # Compute Taxes & Fees = Total - Plan - Equipment - Services - One-Time
        bd["Taxes & Fees"] = (bd["Total"] - bd["Plan"] - bd["Equipment"]
                              - bd["Services"] - bd["One-Time"]).round(2)

        acct_plan = float(acct_rows["plans_charge"].sum()) if not acct_rows.empty else 0
        acct_equip = float(acct_rows["equipment_charge"].sum()) if not acct_rows.empty else 0
        acct_svc = float(acct_rows["services_charge"].sum()) if not acct_rows.empty else 0
        acct_ot = float(acct_rows["onetime_charge"].sum()) if not acct_rows.empty else 0
        acct_tax = round(acct_total - acct_plan - acct_equip - acct_svc - acct_ot, 2)
        acct_r = pd.DataFrame([{
            "Name": "Account", "Phone": "", "Type": "Account", "Status": "",
            "Plan": acct_plan,
            "Equipment": acct_equip,
            "Services": acct_svc,
            "One-Time": acct_ot,
            "Taxes & Fees": acct_tax,
            "Total": acct_total,
        }])
        bd = pd.concat([bd, acct_r], ignore_index=True)

        t_row = pd.DataFrame([{
            "Name": "TOTAL", "Phone": "", "Type": "", "Status": "",
            "Plan": bd["Plan"].sum(), "Equipment": bd["Equipment"].sum(),
            "Services": bd["Services"].sum(), "One-Time": bd["One-Time"].sum(),
            "Taxes & Fees": bd["Taxes & Fees"].sum(),
            "Total": bd["Total"].sum(),
        }])
        bd = pd.concat([bd, t_row], ignore_index=True)

        # Reorder columns: Plan, Equipment, Services, One-Time, Taxes & Fees, Total
        bd = bd[["Name", "Phone", "Type", "Status",
                  "Plan", "Equipment", "Services", "One-Time", "Taxes & Fees", "Total"]]

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
            .format({"Plan": "${:,.2f}", "Equipment": "${:,.2f}",
                      "Services": "${:,.2f}", "One-Time": "${:,.2f}",
                      "Taxes & Fees": "${:,.2f}", "Total": "${:,.2f}"})
            .apply(_style_bd, axis=1),
            use_container_width=True, hide_index=True,
            height=min(800, 60 + 35 * len(bd)),
        )

    # ── TAB 2: Per-Person Splitup ──────────────────────────────────
    with tab2:
        st.subheader("Per-Person Splitup")
        if n_discounted > 0:
            st.caption(
                f"Account **${acct_total:,.2f}** = "
                f"${_PLAN_PREMIUM:.0f} × {n_discounted} discounted line(s) "
                f"+ **${remaining_pool:,.2f}** ÷ {n_voice} voice lines "
                f"= **${equal_share:,.2f}**/line · "
                f"MI & Wearable: own plan only, no account share"
            )
        else:
            st.caption(
                f"Account charge **${acct_total:,.2f}** ÷ **{n_voice}** voice lines "
                f"= **${equal_share:,.2f}** per voice line · "
                f"MI & Wearable: own plan only, no account share"
            )

        # All active lines — Voice gets account share, MI/Wearable gets $0
        sa = active.copy()
        sa["_sort"] = sa["line_type"].map(_type_ord).fillna(3)
        sa["Name"] = sa["phone_number"].map(name_map).fillna(sa["phone_number"])
        sa = sa.sort_values(["_sort", "Name"]).reset_index(drop=True)

        split_rows = []
        voice_idx = 0
        for idx in range(len(sa)):
            r = sa.iloc[idx]
            if r["line_type"] == "Voice":
                voice_idx += 1
                premium = _PLAN_PREMIUM if r["phone_number"] in discounted_phones else 0.0
                acct_sh = premium + equal_share
                # Add rounding remainder to last voice line
                if voice_idx == n_voice:
                    acct_sh += equal_remainder
            else:
                acct_sh = 0.0
            plan_v = float(r["plans_charge"])
            svc_v = float(r["services_charge"])
            equip_v = float(r["equipment_charge"])
            ot_v = float(r["onetime_charge"])
            total_v = float(r["line_total"])
            taxes_v = round(total_v - plan_v - equip_v - svc_v - ot_v, 2)
            split_rows.append({
                "Name": r["Name"],
                "Phone": r["phone_number"],
                "Type": r["line_type"],
                "Plan": plan_v,
                "Services": svc_v,
                "Equipment": equip_v,
                "One-Time": ot_v,
                "Taxes & Fees": taxes_v,
                "Line Total": total_v,
                "Acct Share": round(acct_sh, 2),
                "Month Total": round(total_v + acct_sh, 2),
            })

        sp = pd.DataFrame(split_rows)
        t_sp = pd.DataFrame([{
            "Name": "TOTAL", "Phone": "", "Type": "",
            "Plan": sp["Plan"].sum(), "Services": sp["Services"].sum(),
            "Equipment": sp["Equipment"].sum(), "One-Time": sp["One-Time"].sum(),
            "Taxes & Fees": sp["Taxes & Fees"].sum(),
            "Line Total": sp["Line Total"].sum(),
            "Acct Share": sp["Acct Share"].sum(),
            "Month Total": sp["Month Total"].sum(),
        }])
        sp_disp = pd.concat([sp, t_sp], ignore_index=True)

        money = {c: "${:,.2f}" for c in
                 ["Plan", "Services", "Equipment", "One-Time",
                  "Taxes & Fees", "Line Total", "Acct Share", "Month Total"]}

        def _style_sp(row):
            if row["Name"] == "TOTAL":
                return ["font-weight:bold;background:#E20074;color:white"] * len(row)
            base = ""
            if row["Type"] == "Mobile Internet":
                base = "background:#E3F2FD;"
            elif row["Type"] == "Wearable":
                base = "background:#FFF8E1;"
            styles = [base] * len(row)
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
        total_mt = float(sp_disp.iloc[-1]["Month Total"])
        inv_total = float(inv_sel["total_due"])
        if abs(total_mt - inv_total) < 0.02:
            st.success(
                f"✅ Splitup total **${total_mt:,.2f}** matches invoice **${inv_total:,.2f}**"
            )
        else:
            st.warning(
                f"⚠️ Splitup **${total_mt:,.2f}** vs invoice **${inv_total:,.2f}** "
                f"(diff: ${total_mt - inv_total:+,.2f} — removed/cancelled lines excluded)"
            )

    # ── TAB 3: Distribution ────────────────────────────────────────
    with tab3:
        st.subheader("Cost Distribution")
        col1, col2 = st.columns(2)

        with col1:
            voice_sp = sp[sp["Type"] == "Voice"]
            if not voice_sp.empty:
                fig_pie1 = px.pie(
                    voice_sp, names="Name", values="Month Total",
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
            _a_plan = float(active["plans_charge"].sum())
            _a_equip = float(active["equipment_charge"].sum())
            _a_svc = float(active["services_charge"].sum())
            _a_ot = float(active["onetime_charge"].sum())
            _a_total = float(active["line_total"].sum())
            _a_tax = round(_a_total - _a_plan - _a_equip - _a_svc - _a_ot, 2)
            cats = {
                "Plan": _a_plan,
                "Equipment": _a_equip,
                "Services": _a_svc,
                "One-Time": _a_ot,
                "Taxes & Fees": _a_tax,
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
#  PAGE: Balances (admin only)
# ════════════════════════════════════════════════════════════════════

elif page == "Balances":
    st.title("💰 Balances & Settlements")

    if st.session_state.get("user_role") != "admin":
        st.error("Admin access required.")
        st.stop()

    import requests as _requests
    from datetime import date as _date, datetime as _datetime

    _APP_URL = "https://tmobilebillanalysis-w8t9cas3jwk6u4cc6qqwea.streamlit.app"

    # ── helper: send email via SendGrid ────────────────────────────
    def _send_sg_email(to_email: str, to_name: str, subject: str, html: str):
        sg = st.secrets.get("sendgrid", {})
        api_key = sg.get("api_key", "")
        from_email = sg.get("from_email", "")
        if not api_key or "YOUR_" in api_key:
            return False, "SendGrid API key not configured in secrets."
        if not from_email:
            return False, "Sender email not configured in secrets."
        resp = _requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "personalizations": [{"to": [{"email": to_email, "name": to_name}]}],
                "from": {"email": from_email, "name": "T-Mobile Bill Tracker"},
                "subject": subject,
                "content": [{"type": "text/html", "value": html}],
            },
            timeout=15,
        )
        if resp.status_code in (200, 201, 202):
            return True, "Sent"
        return False, f"SendGrid error {resp.status_code}: {resp.text[:200]}"

    # ── Load accounts & settlements ────────────────────────────────
    _accts_df = run_query("SELECT * FROM accounts ORDER BY account_type, account_name")
    _stl_df   = run_query("SELECT * FROM settlements ORDER BY payment_date DESC")

    # Lookup maps
    _acct_map = {}        # phone -> full row dict
    _primary_phones = []  # list of primary phone numbers
    _admin_phone = None
    for _, ar in _accts_df.iterrows():
        _acct_map[ar["phone_number"]] = {
            "name": ar["account_name"], "type": ar["account_type"],
            "primary_phone": ar["primary_phone"], "relationship": ar["relationship"],
            "email": ar["contact_email"], "is_admin": ar["is_admin"],
        }
        if ar["account_type"] == "Primary":
            _primary_phones.append(ar["phone_number"])
        if ar["is_admin"]:
            _admin_phone = ar["phone_number"]

    _ADMIN_LABEL = _acct_map[_admin_phone]["name"] if _admin_phone else "Admin"

    # Children grouped by primary
    _children_of = {}  # primary_phone -> [child phone rows]
    for _, ar in _accts_df.iterrows():
        if ar["account_type"] == "Child" and ar["primary_phone"]:
            _children_of.setdefault(ar["primary_phone"], []).append(ar)

    # Unassigned phones (in lines table but NOT in accounts)
    _all_lines = run_query("SELECT phone_number, line_type FROM lines ORDER BY line_type, phone_number")
    _registered = set(_accts_df["phone_number"].tolist()) if not _accts_df.empty else set()
    _name_map_bal = (
        persons.drop_duplicates("phone_number")
        .set_index("phone_number")["person_label"].to_dict()
    )
    _unassigned_df = _all_lines[~_all_lines["phone_number"].isin(_registered)]

    # Phones mapped to each primary (primary's own + children)
    def _get_family_phones(primary_phone):
        """Return set of all phone numbers in a primary account's family."""
        phones = {primary_phone}
        for child in _children_of.get(primary_phone, []):
            phones.add(child["phone_number"])
        return phones

    # ── Build full per-line splitup across ALL months ──────────────
    bal_rows = []
    for bd in line_charges["bill_date"].unique():
        lc_m = line_charges[line_charges["bill_date"] == bd]
        sp = _build_month_splitup(lc_m)
        sp["bill_date"] = bd
        sp["month"] = pd.Timestamp(bd).strftime("%b %Y")
        bal_rows.append(sp)
    bal_all = pd.concat(bal_rows, ignore_index=True) if bal_rows else pd.DataFrame()

    if not bal_all.empty:
        pm_map = person_monthly[["phone_number", "bill_date", "person_label"]].drop_duplicates()
        bal_all = bal_all.merge(pm_map, on=["phone_number", "bill_date"], how="left")
        bal_all["person_label"] = bal_all["person_label"].fillna(
            bal_all["phone_number"].map(_name_map_bal)
        ).fillna(bal_all["phone_number"])

    _all_months = sorted(bal_all["month"].unique().tolist(), reverse=True) if not bal_all.empty else []

    # ━━━━ TABS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    tab_accts, tab_summary, tab_pay, tab_history, tab_email = st.tabs([
        "👥 Account Management", "📊 Balance Summary", "💵 Record Payment",
        "📜 Payment History", "✉️ Send Email",
    ])

    # ─────────────────────────────────────────────────────────────
    # TAB 1: Account Management — hierarchical tree view
    # ─────────────────────────────────────────────────────────────
    with tab_accts:
        st.subheader("Account Hierarchy")
        st.caption(
            "**Primary** accounts pay their family share to the T-Mobile admin. "
            "**Child** accounts roll up under a primary. "
            "**Individual** accounts stand alone."
        )

        # ── Tree view ──────────────────────────────────────────────
        if _primary_phones:
            for pp in _primary_phones:
                info = _acct_map[pp]
                badge = " 👑 Admin" if info["is_admin"] else ""
                email_lbl = info["email"] or "no email"
                with st.expander(
                    f"🏠 **{info['name']}** ({pp}){badge}  ·  {email_lbl}",
                    expanded=False,
                ):
                    children = _children_of.get(pp, [])
                    rows_disp = [{"Name": info["name"], "Phone": pp,
                                  "Relationship": "Self", "Type": "Primary"}]
                    for ch in children:
                        rows_disp.append({
                            "Name": ch["account_name"], "Phone": ch["phone_number"],
                            "Relationship": ch["relationship"] or "—",
                            "Type": "Child",
                        })
                    st.dataframe(pd.DataFrame(rows_disp), use_container_width=True, hide_index=True)

                    # Remove a child
                    if children:
                        _ch_map = {f"{ch['account_name']} ({ch['phone_number']})": ch['phone_number'] for ch in children}
                        rc1, rc2, rc3 = st.columns([3, 1, 1])
                        sel_ch = rc1.selectbox("Select child", list(_ch_map.keys()), key=f"rc_{pp}")
                        sel_ch_phone = _ch_map.get(sel_ch, "") if sel_ch else ""
                        if rc2.button("🔓 Unlink", key=f"unlink_{pp}",
                                      help="Convert to Individual (keeps account)"):
                            get_con().execute(
                                "UPDATE accounts SET account_type='Individual', "
                                "primary_phone=NULL, relationship='Self' WHERE phone_number=?",
                                [sel_ch_phone])
                            get_con().execute("CHECKPOINT")
                            st.rerun()
                        if rc3.button("🗑️ Delete", key=f"delch_{pp}",
                                      help="Remove account entirely"):
                            get_con().execute("DELETE FROM accounts WHERE phone_number=?",
                                              [sel_ch_phone])
                            get_con().execute("CHECKPOINT")
                            st.rerun()

        # Individual accounts
        indiv_df = _accts_df[_accts_df["account_type"] == "Individual"]
        if not indiv_df.empty:
            st.markdown("---")
            st.markdown("**📱 Individual Accounts** *(not linked to any primary)*")
            indiv_disp = indiv_df[["account_name", "phone_number", "contact_email"]].rename(columns={
                "account_name": "Name", "phone_number": "Phone", "contact_email": "Email",
            })
            st.dataframe(indiv_disp, use_container_width=True, hide_index=True)

        # Unassigned phones
        if not _unassigned_df.empty:
            st.markdown("---")
            st.markdown(f"**⚠️ Unassigned Phone Lines** ({len(_unassigned_df)})")
            ua_disp = _unassigned_df.copy()
            ua_disp["person_name"] = ua_disp["phone_number"].map(_name_map_bal).fillna("—")
            ua_disp = ua_disp.rename(columns={
                "phone_number": "Phone", "line_type": "Line Type", "person_name": "Known Name",
            })
            st.dataframe(ua_disp[["Phone", "Known Name", "Line Type"]],
                         use_container_width=True, hide_index=True)

        st.divider()

        # ── Create Primary Account ──────────────────────────────
        st.subheader("➕ Create Primary Account")
        # Eligible: unassigned phones + individual accounts
        _eligible_primary = {}  # display_label → phone_number
        for _, r in _unassigned_df.iterrows():
            _pname = _name_map_bal.get(r['phone_number'], '')
            if _pname and _pname != r['phone_number']:
                _eligible_primary[f"{_pname}  ({r['phone_number']}, {r['line_type']})"] = r['phone_number']
            else:
                _eligible_primary[f"{r['phone_number']}  ({r['line_type']})"] = r['phone_number']
        for _, r in indiv_df.iterrows():
            _eligible_primary[f"{r['account_name']}  ({r['phone_number']}, Individual)"] = r['phone_number']

        with st.form("create_primary", clear_on_submit=True):
            cp1, cp2 = st.columns(2)
            if _eligible_primary:
                cp_phone_disp = cp1.selectbox("Phone Line", list(_eligible_primary.keys()), key="cp_phone")
                cp_phone = _eligible_primary.get(cp_phone_disp, "") if cp_phone_disp else ""
            else:
                cp1.info("No unassigned lines available.")
                cp_phone = ""
            cp_name = cp2.text_input("Account Name", key="cp_name")
            cp3, cp4 = st.columns(2)
            cp_email = cp3.text_input("Contact Email (optional)", key="cp_email")
            cp_admin = cp4.checkbox("T-Mobile Admin?", value=False, key="cp_admin")

            if st.form_submit_button("🏠 Create Primary", type="primary"):
                if cp_phone and cp_name.strip():
                    final_name = cp_name.strip()
                    con = get_con()
                    con.execute(
                        "INSERT OR REPLACE INTO accounts VALUES (?, ?, 'Primary', NULL, 'Self', ?, ?)",
                        [cp_phone, final_name, cp_email.strip() or None, cp_admin],
                    )
                    con.execute("CHECKPOINT")
                    st.success(f"✅ Created primary account **{final_name}** ({cp_phone})")
                    st.rerun()
                else:
                    st.warning("Select a phone and enter an account name.")

        # ── Add Child to Primary ────────────────────────────────
        st.divider()
        st.subheader("👶 Add Child Account to a Primary")

        # Eligible for child: unassigned + individual (not already primary with children)
        _eligible_child = {}  # display_label → phone_number
        for _, r in _unassigned_df.iterrows():
            _pname = _name_map_bal.get(r['phone_number'], '')
            if _pname and _pname != r['phone_number']:
                _eligible_child[f"{_pname}  ({r['phone_number']}, {r['line_type']})"] = r['phone_number']
            else:
                _eligible_child[f"{r['phone_number']}  ({r['line_type']})"] = r['phone_number']
        for _, r in indiv_df.iterrows():
            _eligible_child[f"{r['account_name']}  ({r['phone_number']}, Individual)"] = r['phone_number']

        _primary_map = {f"{_acct_map[p]['name']} ({p})": p for p in _primary_phones}

        with st.form("add_child", clear_on_submit=True):
            ac1, ac2 = st.columns(2)
            if _primary_map:
                sel_primary_disp = ac1.selectbox("Parent Primary", list(_primary_map.keys()), key="ac_parent")
                sel_primary_ph = _primary_map.get(sel_primary_disp, "") if sel_primary_disp else ""
            else:
                ac1.warning("Create a Primary account first.")
                sel_primary_ph = ""

            sel_rel = ac2.selectbox("Relationship", [
                "Wife", "Husband", "Son", "Daughter", "Parent",
                "Sibling", "MI Device", "Wearable", "Other",
            ], key="ac_rel")

            if _eligible_child:
                sel_child_disp = st.selectbox("Phone Line", list(_eligible_child.keys()), key="ac_child_phone")
                sel_child_ph = _eligible_child.get(sel_child_disp, "") if sel_child_disp else ""
            else:
                st.info("No unassigned/individual lines available to add.")
                sel_child_ph = ""

            ac_name = st.text_input("Child Name (auto-fill from directory)", value="", key="ac_childname")

            if st.form_submit_button("👶 Add Child", type="primary"):
                if sel_primary_ph and sel_child_ph:
                    final_name = ac_name.strip() or _name_map_bal.get(sel_child_ph, sel_child_ph)
                    con = get_con()
                    con.execute(
                        "INSERT OR REPLACE INTO accounts VALUES (?, ?, 'Child', ?, ?, NULL, FALSE)",
                        [sel_child_ph, final_name, sel_primary_ph, sel_rel],
                    )
                    con.execute("CHECKPOINT")
                    st.success(f"✅ Added **{final_name}** ({sel_child_ph}) as {sel_rel} under "
                               f"**{_acct_map.get(sel_primary_ph, {}).get('name', sel_primary_ph)}**")
                    st.rerun()
                else:
                    st.warning("Select both a primary and a phone line.")

        # ── Reassign Child ──────────────────────────────────────
        all_children = _accts_df[_accts_df["account_type"] == "Child"]
        if not all_children.empty:
            st.divider()
            st.subheader("🔄 Reassign Child to Different Primary")
            with st.form("reassign_child", clear_on_submit=False):
                ra1, ra2 = st.columns(2)
                _child_map = {
                    f"{r['account_name']} ({r['phone_number']})": r['phone_number']
                    for _, r in all_children.iterrows()
                }
                sel_reassign = ra1.selectbox("Child to reassign", list(_child_map.keys()), key="ra_child")
                sel_new_parent_disp = ra2.selectbox("New Primary", list(_primary_map.keys()), key="ra_newparent")
                sel_ra_ph = _child_map.get(sel_reassign, "") if sel_reassign else ""
                sel_new_pp = _primary_map.get(sel_new_parent_disp, "") if sel_new_parent_disp else ""

                if st.form_submit_button("🔄 Reassign", type="primary"):
                    if sel_ra_ph and sel_new_pp:
                        con = get_con()
                        con.execute(
                            "UPDATE accounts SET primary_phone=? WHERE phone_number=?",
                            [sel_new_pp, sel_ra_ph])
                        con.execute("CHECKPOINT")
                        st.success(f"✅ Reassigned to **{_acct_map.get(sel_new_pp, {}).get('name', '')}**")
                        st.rerun()

        # ── Edit Account Details ────────────────────────────────
        st.divider()
        st.subheader("✏️ Edit Account Details")
        _all_acct_map = {
            f"{r['account_name']} ({r['phone_number']})": r['phone_number']
            for _, r in _accts_df.iterrows()
        }
        if _all_acct_map:
            sel_edit_disp = st.selectbox("Select account to edit", list(_all_acct_map.keys()), key="edit_sel")
            sel_edit_ph = _all_acct_map.get(sel_edit_disp, "") if sel_edit_disp else ""
            if sel_edit_ph and sel_edit_ph in _acct_map:
                cur = _acct_map[sel_edit_ph]
                with st.form("edit_account", clear_on_submit=False):
                    ed1, ed2 = st.columns(2)
                    ed_name = ed1.text_input("Name", value=cur["name"], key="ed_name")
                    ed_email = ed2.text_input("Email", value=cur["email"] or "", key="ed_email")
                    if st.form_submit_button("💾 Update", type="primary"):
                        con = get_con()
                        con.execute(
                            "UPDATE accounts SET account_name=?, contact_email=? WHERE phone_number=?",
                            [ed_name.strip(), ed_email.strip() or None, sel_edit_ph])
                        con.execute("CHECKPOINT")
                        st.success(f"✅ Updated **{ed_name.strip()}**")
                        st.rerun()

        # ── Delete Primary Account ──────────────────────────────
        non_admin_primaries = [p for p in _primary_phones
                               if not _acct_map[p].get("is_admin", False)]
        if non_admin_primaries:
            st.divider()
            st.subheader("🗑️ Delete Primary Account")
            st.caption("Deleting a primary converts all its children to Individual.")
            _del_map = {f"{_acct_map[p]['name']} ({p})": p for p in non_admin_primaries}
            del_sel = st.selectbox("Primary to delete", list(_del_map.keys()), key="del_prim_sel")
            del_ph = _del_map.get(del_sel, "") if del_sel else ""
            if st.button("🗑️ Delete Primary", key="del_prim_btn"):
                con = get_con()
                # Convert children to Individual
                con.execute(
                    "UPDATE accounts SET account_type='Individual', primary_phone=NULL, "
                    "relationship='Self' WHERE primary_phone=?", [del_ph])
                con.execute("DELETE FROM accounts WHERE phone_number=?", [del_ph])
                con.execute("DELETE FROM settlements WHERE primary_phone=?", [del_ph])
                con.execute("CHECKPOINT")
                st.success(f"Deleted **{_acct_map[del_ph]['name']}**. Children converted to Individual.")
                st.rerun()

    # ─────────────────────────────────────────────────────────────
    # TAB 2: Balance Summary — per primary account
    # ─────────────────────────────────────────────────────────────
    with tab_summary:
        st.subheader("Account Balances")
        st.caption(f"Each non-admin primary account owes their family total to **{_ADMIN_LABEL}**.")

        month_filter = st.multiselect(
            "Filter by months (leave empty = all months)", _all_months,
            key="bal_month_filter",
        )

        if bal_all.empty:
            st.info("No bill data.")
        else:
            filtered = bal_all.copy()
            if month_filter:
                filtered = filtered[filtered["month"].isin(month_filter)]

            # Build per-primary aggregation
            summary_rows = []
            for pp in _primary_phones:
                family = _get_family_phones(pp)
                fam_data = filtered[filtered["phone_number"].isin(family)]
                total_owed = float(fam_data["month_total"].sum()) if not fam_data.empty else 0.0
                # Settlements
                if not _stl_df.empty:
                    stl_pp = _stl_df[_stl_df["primary_phone"] == pp]
                    if month_filter:
                        stl_pp = stl_pp[stl_pp["bill_month"].isin(month_filter) | stl_pp["bill_month"].isna()]
                    total_paid = float(stl_pp["amount"].sum())
                else:
                    total_paid = 0.0
                info = _acct_map[pp]
                summary_rows.append({
                    "primary_phone": pp, "account": info["name"],
                    "is_admin": info["is_admin"],
                    "email": info["email"] or "—",
                    "members": len(family),
                    "total_owed": round(total_owed, 2),
                    "total_paid": round(total_paid, 2),
                    "balance": round(total_owed - total_paid, 2),
                })

            balance_df = pd.DataFrame(summary_rows)
            if balance_df.empty:
                st.info("No primary accounts configured.")
            else:
                balance_df = balance_df.sort_values("balance", ascending=False).reset_index(drop=True)
                non_admin = balance_df[~balance_df["is_admin"]]

                # Metrics
                if not non_admin.empty:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Total Outstanding", f"${non_admin['balance'].sum():,.2f}")
                    c2.metric("Total Collected", f"${non_admin['total_paid'].sum():,.2f}")
                    c3.metric("Total Owed", f"${non_admin['total_owed'].sum():,.2f}")

                pfx = "📊 Filtered: " + ", ".join(month_filter) if month_filter else "📊 All Months"
                st.caption(pfx)

                disp = balance_df.copy()
                disp["type"] = disp["is_admin"].map({True: "👑 Admin", False: "👤 Primary"})
                disp = disp.rename(columns={
                    "account": "Account", "total_owed": "Total Owed",
                    "total_paid": "Total Paid", "balance": "Balance Due",
                    "email": "Email", "type": "Type", "members": "Lines",
                })
                disp_cols = ["Account", "Type", "Lines", "Total Owed", "Total Paid", "Balance Due", "Email"]

                def _color_bal(val):
                    if isinstance(val, (int, float)):
                        if val > 0:
                            return "color: #E20074; font-weight: bold"
                        elif val == 0:
                            return "color: #00C853; font-weight: bold"
                    return ""

                st.dataframe(
                    disp[disp_cols].style.applymap(_color_bal, subset=["Balance Due"]).format(
                        {"Total Owed": "${:,.2f}", "Total Paid": "${:,.2f}", "Balance Due": "${:,.2f}"}
                    ),
                    use_container_width=True, hide_index=True,
                )

                # Bar chart (non-admin)
                if not non_admin.empty:
                    fig = px.bar(
                        non_admin.sort_values("balance", ascending=True),
                        x="balance", y="account", orientation="h",
                        color="balance",
                        color_continuous_scale=["#00C853", "#FFB900", "#E20074"],
                        title="Outstanding Balance by Primary Account",
                        labels={"balance": "Balance Due ($)", "account": "Account"},
                    )
                    fig.update_layout(height=max(300, len(non_admin) * 60), showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                # ── Drill-down per primary ─────────────────────────
                st.subheader("Account Drill-down")
                _dd_map = {f"{r['account']} ({r['primary_phone']})": r['primary_phone'] for _, r in balance_df.iterrows()}
                sel_dd = st.selectbox("Select primary account", list(_dd_map.keys()), key="bal_acct_dd")
                sel_dd_ph = _dd_map.get(sel_dd, "") if sel_dd else ""

                if sel_dd_ph:
                    family_ph = _get_family_phones(sel_dd_ph)
                    fam_data = filtered[filtered["phone_number"].isin(family_ph)].copy()
                    if not fam_data.empty:
                        # Per-member totals
                        st.markdown("**👥 Family Members:**")
                        mem_tot = fam_data.groupby(["person_label", "phone_number"]).agg(
                            plan=("plan", "sum"), equipment=("equipment", "sum"),
                            services=("services", "sum"), onetime=("onetime", "sum"),
                            taxes_fees=("taxes_fees", "sum"), line_total=("line_total", "sum"),
                            acct_share=("acct_share", "sum"), month_total=("month_total", "sum"),
                        ).reset_index()
                        # Attach relationship
                        mem_tot["relationship"] = mem_tot["phone_number"].map(
                            lambda ph: _acct_map.get(ph, {}).get("relationship", "—")
                        )
                        mem_disp = mem_tot.rename(columns={
                            "person_label": "Name", "phone_number": "Phone",
                            "relationship": "Relation", "plan": "Plan",
                            "equipment": "Equipment", "services": "Services",
                            "onetime": "One-Time", "taxes_fees": "Taxes & Fees",
                            "line_total": "Line Total", "acct_share": "Acct Share",
                            "month_total": "Month Total",
                        })
                        st.dataframe(
                            mem_disp[["Name", "Phone", "Relation", "Plan", "Equipment",
                                      "Services", "One-Time", "Taxes & Fees", "Line Total",
                                      "Acct Share", "Month Total"]].style.format({
                                "Plan": "${:,.2f}", "Equipment": "${:,.2f}",
                                "Services": "${:,.2f}", "One-Time": "${:,.2f}",
                                "Taxes & Fees": "${:,.2f}", "Line Total": "${:,.2f}",
                                "Acct Share": "${:,.2f}", "Month Total": "${:,.2f}",
                            }),
                            use_container_width=True, hide_index=True,
                        )

                        # Monthly breakdown
                        st.markdown("**📅 Monthly Breakdown:**")
                        mo_tot = fam_data.groupby(["month", "bill_date"]).agg(
                            plan=("plan", "sum"), equipment=("equipment", "sum"),
                            services=("services", "sum"), onetime=("onetime", "sum"),
                            taxes_fees=("taxes_fees", "sum"), line_total=("line_total", "sum"),
                            acct_share=("acct_share", "sum"), month_total=("month_total", "sum"),
                        ).reset_index().sort_values("bill_date")
                        if not _stl_df.empty:
                            stl_dd = _stl_df[_stl_df["primary_phone"] == sel_dd_ph]
                            stl_mo = stl_dd.groupby("bill_month")["amount"].sum().reset_index().rename(
                                columns={"amount": "paid", "bill_month": "month"})
                            mo_tot = mo_tot.merge(stl_mo, on="month", how="left")
                            mo_tot["paid"] = mo_tot["paid"].fillna(0)
                        else:
                            mo_tot["paid"] = 0.0
                        mo_tot["balance"] = (mo_tot["month_total"] - mo_tot["paid"]).round(2)
                        mo_disp = mo_tot[["month", "plan", "equipment", "services", "onetime",
                                          "taxes_fees", "line_total", "acct_share", "month_total",
                                          "paid", "balance"]].rename(columns={
                            "month": "Month", "plan": "Plan", "equipment": "Equipment",
                            "services": "Services", "onetime": "One-Time",
                            "taxes_fees": "Taxes & Fees", "line_total": "Line Total",
                            "acct_share": "Acct Share", "month_total": "Month Total",
                            "paid": "Paid", "balance": "Balance",
                        })
                        st.dataframe(
                            mo_disp.style.format({
                                "Plan": "${:,.2f}", "Equipment": "${:,.2f}",
                                "Services": "${:,.2f}", "One-Time": "${:,.2f}",
                                "Taxes & Fees": "${:,.2f}", "Line Total": "${:,.2f}",
                                "Acct Share": "${:,.2f}", "Month Total": "${:,.2f}",
                                "Paid": "${:,.2f}", "Balance": "${:,.2f}",
                            }),
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.info("No charge data for the selected filters / account.")

    # ─────────────────────────────────────────────────────────────
    # TAB 3: Record Payment
    # ─────────────────────────────────────────────────────────────
    with tab_pay:
        st.subheader("Record a Payment")
        st.caption("Log a payment received from a primary account toward their family's bill share.")

        pay_primaries = [p for p in _primary_phones
                         if not _acct_map[p].get("is_admin", False)]
        _pay_map = {f"{_acct_map[p]['name']} ({p})": p for p in pay_primaries}

        with st.form("record_payment", clear_on_submit=True):
            pc1, pc2 = st.columns(2)
            pay_sel = pc1.selectbox("Primary Account", list(_pay_map.keys()), key="pay_acct_sel") if _pay_map else ""
            pay_amount = pc2.number_input("Amount ($)", min_value=0.01, step=10.0, format="%.2f")

            pc3, pc4 = st.columns(2)
            pay_date = pc3.date_input("Payment Date", value=_date.today())
            pay_month = pc4.selectbox("For Bill Month (optional)", ["— General —"] + _all_months)

            pc5, pc6 = st.columns(2)
            pay_method = pc5.selectbox("Method", ["Zelle", "Cash", "Venmo", "Check", "Other"])
            pay_notes = pc6.text_input("Notes (optional)")

            submitted = st.form_submit_button("💾 Save Payment", type="primary")
            if submitted and pay_sel and pay_amount > 0:
                pay_ph = _pay_map.get(pay_sel, "") if pay_sel else ""
                bill_month_val = None if pay_month == "— General —" else pay_month
                recorder = st.session_state.get("user_email", "unknown")
                con = get_con()
                con.execute("""
                    INSERT INTO settlements (settlement_id, primary_phone, bill_month,
                        amount, payment_date, payment_method, notes, recorded_by)
                    VALUES (nextval('seq_settlement'), ?, ?, ?, ?, ?, ?, ?)
                """, [pay_ph, bill_month_val, pay_amount,
                      str(pay_date), pay_method, pay_notes, recorder])
                con.execute("CHECKPOINT")
                acct_name = _acct_map.get(pay_ph, {}).get("name", pay_ph)
                st.success(f"✅ Recorded **${pay_amount:,.2f}** from **{acct_name}** on {pay_date}.")
                st.rerun()

    # ─────────────────────────────────────────────────────────────
    # TAB 4: Payment History
    # ─────────────────────────────────────────────────────────────
    with tab_history:
        st.subheader("Payment History")
        if _stl_df.empty:
            st.info("No payments recorded yet.")
        else:
            stl_disp = _stl_df.copy()
            stl_disp["account"] = stl_disp["primary_phone"].map(
                lambda p: _acct_map.get(p, {}).get("name", p)
            )
            stl_disp = stl_disp.rename(columns={
                "settlement_id": "ID", "account": "Account",
                "bill_month": "Bill Month", "amount": "Amount",
                "payment_date": "Date", "payment_method": "Method",
                "notes": "Notes", "recorded_by": "Recorded By",
            })
            stl_disp = stl_disp[["ID", "Account", "Bill Month", "Amount",
                                  "Date", "Method", "Notes", "Recorded By"]]
            stl_disp["Bill Month"] = stl_disp["Bill Month"].fillna("General")
            st.dataframe(
                stl_disp.style.format({"Amount": "${:,.2f}"}),
                use_container_width=True, hide_index=True,
            )

            st.markdown("---")
            st.subheader("Delete a Payment")
            del_ids = _stl_df["settlement_id"].tolist()
            del_id = st.selectbox("Settlement ID to delete", del_ids, key="del_stl")
            if st.button("🗑️ Delete", key="del_stl_btn"):
                get_con().execute("DELETE FROM settlements WHERE settlement_id = ?", [del_id])
                get_con().execute("CHECKPOINT")
                st.success(f"Deleted settlement #{del_id}.")
                st.rerun()

    # ─────────────────────────────────────────────────────────────
    # TAB 5: Send Email — selective months, per primary account
    # ─────────────────────────────────────────────────────────────
    with tab_email:
        st.subheader("Send Bill Email")
        st.caption("Send a detailed bill email to a primary account for selected months with optional notes.")

        emailable = [p for p in _primary_phones if _acct_map[p].get("email")]
        if not emailable:
            st.warning("No primary accounts with email addresses. Set emails in Account Management.")
        else:
            _em_map = {f"{_acct_map[p]['name']} ({p})": p for p in emailable}
            em_sel = st.selectbox("Send to", list(_em_map.keys()), key="em_acct_sel")
            em_ph = _em_map.get(em_sel, "") if em_sel else ""
            em_months = st.multiselect("Select months to include", _all_months, key="em_months")
            em_notes = st.text_area("Personal note (included in the email)", height=80, key="em_notes",
                                    placeholder="e.g. Please pay by March 15th via Zelle to ...")

            if st.button("📧 Send Email", type="primary", key="em_send_btn"):
                if not em_months:
                    st.warning("Select at least one month.")
                elif em_ph:
                    family = _get_family_phones(em_ph)
                    em_data = bal_all[
                        (bal_all["phone_number"].isin(family)) &
                        (bal_all["month"].isin(em_months))
                    ].copy() if not bal_all.empty else pd.DataFrame()

                    if em_data.empty:
                        st.warning("No bill data for this account in the selected months.")
                    else:
                        em_name = _acct_map[em_ph]["name"]
                        em_email = _acct_map[em_ph]["email"]

                        # Per-month
                        em_mo = em_data.groupby("month")["month_total"].sum().reset_index()
                        stl_em = _stl_df[
                            (_stl_df["primary_phone"] == em_ph) &
                            (_stl_df["bill_month"].isin(em_months))
                        ] if not _stl_df.empty else pd.DataFrame()
                        stl_em_sum = stl_em.groupby("bill_month")["amount"].sum().to_dict() if not stl_em.empty else {}

                        # Per-member
                        mem_detail = em_data.groupby(["person_label", "phone_number"])["month_total"].sum().reset_index()
                        mem_detail["relationship"] = mem_detail["phone_number"].map(
                            lambda ph: _acct_map.get(ph, {}).get("relationship", "—"))

                        # Build HTML
                        months_rows = ""
                        grand_owed = grand_paid = 0
                        for _, mr in em_mo.iterrows():
                            m = mr["month"]
                            owed = mr["month_total"]
                            paid = stl_em_sum.get(m, 0)
                            bal = owed - paid
                            grand_owed += owed; grand_paid += paid
                            bg = ' style="background:#FFF0F5"' if bal > 0 else ""
                            months_rows += f"""<tr{bg}>
                                <td style="padding:6px;border:1px solid #ddd">{m}</td>
                                <td style="padding:6px;border:1px solid #ddd;text-align:right">${owed:,.2f}</td>
                                <td style="padding:6px;border:1px solid #ddd;text-align:right">${paid:,.2f}</td>
                                <td style="padding:6px;border:1px solid #ddd;text-align:right;font-weight:bold">${bal:,.2f}</td>
                            </tr>"""
                        grand_bal = grand_owed - grand_paid

                        members_rows = ""
                        for _, md in mem_detail.iterrows():
                            members_rows += f"""<tr>
                                <td style="padding:5px;border:1px solid #ddd">{md['person_label']}</td>
                                <td style="padding:5px;border:1px solid #ddd">{md['phone_number']}</td>
                                <td style="padding:5px;border:1px solid #ddd">{md['relationship']}</td>
                                <td style="padding:5px;border:1px solid #ddd;text-align:right">${md['month_total']:,.2f}</td>
                            </tr>"""

                        notes_html = (f'<div style="background:#FFFDE7;padding:12px;border-left:4px solid #FFB900;'
                                      f'margin:16px 0"><strong>Note:</strong> {em_notes}</div>'
                                      if em_notes.strip() else "")

                        html_body = f"""
                        <div style="font-family:Arial,sans-serif;max-width:650px;margin:auto">
                          <h2 style="color:#E20074">T-Mobile Bill Summary</h2>
                          <p>Hi <strong>{em_name}</strong>,</p>
                          <p>Here is your T-Mobile bill share for <strong>{', '.join(em_months)}</strong>:</p>
                          {notes_html}
                          <h3 style="color:#333">Monthly Breakdown</h3>
                          <table style="border-collapse:collapse;width:100%">
                            <tr style="background:#E20074;color:white">
                              <th style="padding:8px;border:1px solid #ddd;text-align:left">Month</th>
                              <th style="padding:8px;border:1px solid #ddd;text-align:right">Owed</th>
                              <th style="padding:8px;border:1px solid #ddd;text-align:right">Paid</th>
                              <th style="padding:8px;border:1px solid #ddd;text-align:right">Balance</th>
                            </tr>
                            {months_rows}
                            <tr style="background:#333;color:white">
                              <td style="padding:8px;border:1px solid #ddd"><strong>TOTAL</strong></td>
                              <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${grand_owed:,.2f}</strong></td>
                              <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${grand_paid:,.2f}</strong></td>
                              <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${grand_bal:,.2f}</strong></td>
                            </tr>
                          </table>
                          <h3 style="color:#333;margin-top:20px">Your Lines</h3>
                          <table style="border-collapse:collapse;width:100%">
                            <tr style="background:#666;color:white">
                              <th style="padding:6px;border:1px solid #ddd">Name</th>
                              <th style="padding:6px;border:1px solid #ddd">Phone</th>
                              <th style="padding:6px;border:1px solid #ddd">Relation</th>
                              <th style="padding:6px;border:1px solid #ddd;text-align:right">Total</th>
                            </tr>
                            {members_rows}
                          </table>
                          <p style="margin-top:16px">Please reach out to <strong>{_ADMIN_LABEL}</strong> for payment details.</p>
                          <p style="color:#888;font-size:12px">Sent from <a href="{_APP_URL}">T-Mobile Bill Tracker</a></p>
                        </div>"""

                        ok, msg = _send_sg_email(em_email, em_name,
                                                 f"T-Mobile Bill – {', '.join(em_months)}", html_body)
                        if ok:
                            st.success(f"✅ Email sent to **{em_name}** ({em_email})")
                        else:
                            st.error(f"❌ {msg}")

            # ── Master summary to admin ───────────────────────────
            st.divider()
            st.subheader("Send Master Summary to Admin")
            admin_email = _acct_map.get(_admin_phone, {}).get("email", "") if _admin_phone else ""
            if admin_email and _all_months:
                ms_months = st.multiselect("Months for summary", _all_months, key="ms_months")
                if st.button(f"📧 Send Master Summary to {_ADMIN_LABEL}", key="ms_send"):
                    if not ms_months:
                        st.warning("Select at least one month.")
                    else:
                        ms_filtered = bal_all[bal_all["month"].isin(ms_months)] if not bal_all.empty else pd.DataFrame()
                        if ms_filtered.empty:
                            st.warning("No data for those months.")
                        else:
                            rows_data = []
                            for pp in _primary_phones:
                                fam = _get_family_phones(pp)
                                fam_d = ms_filtered[ms_filtered["phone_number"].isin(fam)]
                                owed = float(fam_d["month_total"].sum()) if not fam_d.empty else 0
                                paid = 0
                                if not _stl_df.empty:
                                    sp = _stl_df[(_stl_df["primary_phone"] == pp) &
                                                 (_stl_df["bill_month"].isin(ms_months) | _stl_df["bill_month"].isna())]
                                    paid = float(sp["amount"].sum())
                                rows_data.append({
                                    "Account": _acct_map[pp]["name"],
                                    "Owed": round(owed, 2), "Paid": round(paid, 2),
                                    "Balance": round(owed - paid, 2),
                                })
                            ms_df = pd.DataFrame(rows_data)
                            rows_h = ""
                            for _, sr in ms_df.iterrows():
                                bg = ' style="background:#FFF0F5"' if sr["Balance"] > 0 else ""
                                rows_h += f"""<tr{bg}>
                                    <td style="padding:6px;border:1px solid #ddd">{sr['Account']}</td>
                                    <td style="padding:6px;border:1px solid #ddd;text-align:right">${sr['Owed']:,.2f}</td>
                                    <td style="padding:6px;border:1px solid #ddd;text-align:right">${sr['Paid']:,.2f}</td>
                                    <td style="padding:6px;border:1px solid #ddd;text-align:right;font-weight:bold">${sr['Balance']:,.2f}</td>
                                </tr>"""
                            ms_html = f"""
                            <div style="font-family:Arial,sans-serif;max-width:650px;margin:auto">
                              <h2 style="color:#E20074">T-Mobile Bill – Master Summary</h2>
                              <p>Hi <strong>{_ADMIN_LABEL}</strong>,</p>
                              <p>Balance summary for <strong>{', '.join(ms_months)}</strong>:</p>
                              <table style="border-collapse:collapse;width:100%">
                                <tr style="background:#E20074;color:white">
                                  <th style="padding:8px;border:1px solid #ddd;text-align:left">Account</th>
                                  <th style="padding:8px;border:1px solid #ddd;text-align:right">Owed</th>
                                  <th style="padding:8px;border:1px solid #ddd;text-align:right">Paid</th>
                                  <th style="padding:8px;border:1px solid #ddd;text-align:right">Balance</th>
                                </tr>
                                {rows_h}
                                <tr style="background:#333;color:white">
                                  <td style="padding:8px;border:1px solid #ddd"><strong>TOTAL</strong></td>
                                  <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${ms_df['Owed'].sum():,.2f}</strong></td>
                                  <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${ms_df['Paid'].sum():,.2f}</strong></td>
                                  <td style="padding:8px;border:1px solid #ddd;text-align:right"><strong>${ms_df['Balance'].sum():,.2f}</strong></td>
                                </tr>
                              </table>
                              <p style="margin-top:16px"><a href="{_APP_URL}">Open Dashboard →</a></p>
                            </div>"""
                            ok, msg = _send_sg_email(admin_email, _ADMIN_LABEL,
                                                     f"T-Mobile Master Summary – {', '.join(ms_months)}", ms_html)
                            if ok:
                                st.success(f"✅ Master summary sent to {admin_email}")
                            else:
                                st.error(f"❌ {msg}")
            elif not admin_email:
                st.info(f"Set an email for **{_ADMIN_LABEL}** in Account Management to enable this.")


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

