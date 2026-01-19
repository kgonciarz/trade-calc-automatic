# app.py — Incoterm Auto Calculator (single "Price") + ICE London LIVE
# Result = Price − TotalCosts + Diff
# Freight uses FREIGHT + LINER + SURCHARGE per container, and user selects Shipping Line.

from dotenv import load_dotenv
load_dotenv()

import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf

# =========================
# CONFIG
# =========================
BASE_SYMBOL = "£"
INCOTERMS = ["EXW", "FCA", "FOB", "CFR", "CIF", "DAP", "DDP"]

COST_ITEMS_XLSX = "cost_items.xlsx"
INCOTERM_MATRIX_XLSX = "incoterm_matrix.xlsx"
FREIGHT_XLSX = "logistics_freight_trade_calc.xlsx"
WAREHOUSE_XLSX = "warehouse_costs.xlsx"

RENT_ALIASES = {"WAREHOUSE RENT", "RENT", "STORAGE RENT"}
COCOA_DELIVERY_MONTHS = [("Mar", "H"), ("May", "K"), ("Jul", "N"), ("Sep", "U"), ("Dec", "Z")]

ICE_XTICK_URL = os.getenv("ICE_XTICK_URL", "")
ICE_USERNAME  = os.getenv("ICE_USERNAME", "")
ICE_PASSWORD  = os.getenv("ICE_PASSWORD", "")

def _ice_ok() -> bool:
    return bool(ICE_XTICK_URL and ICE_USERNAME and ICE_PASSWORD)

# =========================
# FALLBACK DROPDOWNS (kept)
# =========================
pol_options = [
    "POL", "ABIDJAN", "TIN CAN", "APAPA", "CALLAO", "CONAKRY", "DIEGO SUAREZ", "DOUALA",
    "FREETOWN", "KAMPALA", "KRIBI", "LEKKI", "LOME", "MATADI", "MOMBASA", "MONROVIA",
    "NOSY BE", "SAN PEDRO", "TAKORADI", "TEMA", "CARTAGENA", "GUAYAQUIL", "POSORJA",
    "PAITA", "CAUCEDO", "ANTWERP", "KINSHASA", "LAGOS", "PISCO"
]
destination_options = [
    "ANTWERP", "BARCELONA", "AMSTERDAM", "HAMBURG", "ISTANBUL", "ROTTERDAM", "VALENCIA",
    "BATAM", "PASIR GUDANG", "SURABAYA", "PTP", "PHILADELPHIA", "SZCZECIN", "WELLINGTON",
    "AMBARLI", "GENOA", "VADO LIGURE", "SINGAPORE", "TALLINN", "JAKARTA", "PORT KLANG",
    "NEW YORK", "MONTREAL", "PIRAEUS", "YOKOHAMA", "VALENCIA", "BATAM VIA SINGAPORE",
    "SHANGHAI", "KLAIPEDA", "LIVERPOOL"
]

# =========================
# HELPERS
# =========================
def _norm(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

def _money_to_float(x) -> float:
    """
    Robust parsing for cells like:
      250
      "250"
      "$250.00"
      " $250.00 "
      "1,234.56"
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0
    # keep digits, dot, minus, comma
    s = re.sub(r"[^0-9\-\.,]", "", s)
    if s.count(",") > 0 and s.count(".") == 0:
        # treat comma as decimal only if no dot exists (rare)
        s = s.replace(",", ".")
    # remove thousands commas
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False, ttl=300)
def get_fx_rate(pair: str) -> float | None:
    try:
        t = yf.Ticker(pair)
        h = t.history(period="1d")
        if not h.empty:
            return float(h["Close"].iloc[-1])
    except Exception:
        return None
    return None

eur_gbp_rate = get_fx_rate("EURGBP=X") or 0.85
usd_gbp_rate = get_fx_rate("USDGBP=X") or 0.79

def to_gbp(amount: float, ccy: str) -> float:
    c = (ccy or "GBP").upper()
    if c == "GBP":
        return float(amount)
    if c == "EUR":
        return float(amount) * eur_gbp_rate
    if c == "USD":
        return float(amount) * usd_gbp_rate
    return float(amount)

# =========================
# ICE xtick
# =========================
@st.cache_data(ttl=15, show_spinner=False)
def fetch_ice_last_close(symbol: str) -> float | None:
    if not _ice_ok():
        return None

    r = requests.get(
        ICE_XTICK_URL,
        params={
            "username": ICE_USERNAME,
            "pwd": ICE_PASSWORD,
            "symbol": symbol,
            "period": "i5",
            "options.nbars": "1",
        },
        timeout=20,
    )
    r.raise_for_status()

    xml_text = (r.text or "").strip()
    if not xml_text:
        return None
    if "not entitled" in xml_text.lower():
        return None

    root = ET.fromstring(xml_text)
    bar = root.find(".//bar")
    if bar is None:
        return None
    close_txt = bar.findtext("close")
    if close_txt is None:
        return None

    val = pd.to_numeric(close_txt, errors="coerce")
    return None if pd.isna(val) else float(val)

# =========================
# LOAD COST + MATRIX
# =========================
@st.cache_data(show_spinner=False)
def load_cost_tables():
    cost_df = pd.read_excel(COST_ITEMS_XLSX)
    mat_df  = pd.read_excel(INCOTERM_MATRIX_XLSX)

    cost_df.columns = [str(c).strip().upper() for c in cost_df.columns]
    mat_df.columns  = [str(c).strip().upper() for c in mat_df.columns]

    cost_df["KEY"] = cost_df["COST ITEM"].map(_norm)
    mat_df["KEY"]  = mat_df["COST ITEM"].map(_norm)

    cost_df["VALUE"] = pd.to_numeric(cost_df.get("VALUE"), errors="coerce")
    cost_df["TYPE"] = cost_df.get("TYPE", "").astype(str).str.strip().str.lower().replace(
        {"percentage": "percent", "%": "percent"}
    )

    for ic in INCOTERMS:
        if ic not in mat_df.columns:
            mat_df[ic] = 0
        mat_df[ic] = pd.to_numeric(mat_df[ic], errors="coerce").fillna(0).astype(int)

    return cost_df, mat_df

def included_keys(mat_df: pd.DataFrame, incoterm: str) -> list[str]:
    ic = incoterm.strip().upper()
    keys = mat_df.loc[mat_df[ic] == 1, "KEY"].tolist()
    return list(dict.fromkeys(keys))

# =========================
# FREIGHT (FREIGHT + LINER + SURCHARGE, user selects shipping line)
# =========================
@st.cache_data(show_spinner=False)
def load_freight_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)

    def norm_col(c):
        return re.sub(r"\s+", " ", str(c or "")).strip().upper()

    df.columns = [norm_col(c) for c in df.columns]

    if "SHIPPING LINE" not in df.columns and "SHIPPING LINE S" in df.columns:
        df = df.rename(columns={"SHIPPING LINE S": "SHIPPING LINE"})

    # FREIGHT file often has weird spaces in headers; after norm_col it's clean
    needed = {"POL", "POD", "CONTAINER", "SHIPPING LINE", "FREIGHT", "CURRENCY"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Freight file missing columns: {missing}")

    # Optional columns
    if "LINER" not in df.columns:
        df["LINER"] = 0
    if "SURCHARGE" not in df.columns:
        df["SURCHARGE"] = 0

    df["POL"] = df["POL"].map(_norm)
    df["POD"] = df["POD"].map(_norm)
    df["CONTAINER"] = df["CONTAINER"].astype(str).str.strip()
    df["SHIPPING LINE"] = df["SHIPPING LINE"].map(_norm)
    df["CURRENCY"] = df["CURRENCY"].astype(str).str.strip().str.upper()

    df["FREIGHT"] = df["FREIGHT"].apply(_money_to_float)
    df["LINER"] = df["LINER"].apply(_money_to_float)
    df["SURCHARGE"] = df["SURCHARGE"].apply(_money_to_float)

    df = df.dropna(subset=["POL", "POD", "CONTAINER", "SHIPPING LINE", "CURRENCY"]).copy()
    return df

def freight_gbp_per_ton(
    df: pd.DataFrame,
    pol: str,
    pod: str,
    container: str,
    shipping_line: str,
) -> tuple[float | None, dict]:
    """
    Returns (gbp_per_ton, detail_dict).
    Per-container total = FREIGHT + LINER + SURCHARGE.
    Uses row currency (EUR/USD/GBP) for conversion.
    """
    pol_n = _norm(pol)
    pod_n = _norm(pod)
    cont = str(container).strip()
    sl = _norm(shipping_line)

    sub = df[(df["POL"] == pol_n) & (df["POD"] == pod_n) & (df["CONTAINER"] == cont) & (df["SHIPPING LINE"] == sl)].copy()
    if sub.empty:
        return None, {}

    # If multiple rows for same line, pick the first (or you can pick max by total)
    sub["TOTAL_CONTAINER"] = sub["FREIGHT"] + sub["LINER"] + sub["SURCHARGE"]
    chosen = sub.loc[sub["TOTAL_CONTAINER"].idxmax()]  # safest if duplicates

    ccy = str(chosen["CURRENCY"]).upper()
    total_container = float(chosen["TOTAL_CONTAINER"])
    freight_container = float(chosen["FREIGHT"])
    liner_container = float(chosen["LINER"])
    surcharge_container = float(chosen["SURCHARGE"])

    if ccy == "EUR":
        conv = eur_gbp_rate
    elif ccy == "USD":
        conv = usd_gbp_rate
    else:
        conv = 1.0

    total_gbp_container = total_container * conv

    tons_per_container = 20.0 if cont == "20" else 40.0
    gbp_per_ton = round(total_gbp_container / tons_per_container, 2)

    details = {
        "currency": ccy,
        "conv_to_gbp": conv,
        "freight_container": freight_container,
        "liner_container": liner_container,
        "surcharge_container": surcharge_container,
        "total_container": total_container,
        "total_gbp_container": total_gbp_container,
        "tons_per_container": tons_per_container,
    }
    return gbp_per_ton, details

# =========================
# WAREHOUSE
# =========================
@st.cache_data(show_spinner=False)
def load_warehouse_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, index_col=0)
    df.index = df.index.map(_norm)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def warehouse_series(wh_df: pd.DataFrame, wh_name: str, rent_months: int) -> pd.Series:
    if wh_name not in wh_df.columns:
        raise ValueError(f"Warehouse '{wh_name}' not found")
    s = pd.to_numeric(wh_df[wh_name], errors="coerce").fillna(0.0).astype(float)
    s.index = s.index.map(_norm)
    rent_key = next((k for k in RENT_ALIASES if k in s.index), None)
    if rent_key:
        s.loc[rent_key] = float(s.loc[rent_key]) * int(rent_months)
    return s

# =========================
# UNIFIED TABLE ENGINE
# =========================
def build_unified_cost_table(
    *,
    inc_keys: list[str],
    price_gbp: float,
    cost_df: pd.DataFrame,
    computed: dict[str, float],
    manual_missing: dict[str, float],
) -> tuple[pd.DataFrame, float]:
    inc_set = set(inc_keys)
    cost_map = {r["KEY"]: r for _, r in cost_df.iterrows()}

    keys_to_show = list(dict.fromkeys(
        inc_keys
        + [_norm(k) for k in computed.keys()]
        + [_norm(k) for k in manual_missing.keys()]
    ))

    def find_orig(d: dict[str, float], key_norm: str) -> str | None:
        for k in d.keys():
            if _norm(k) == key_norm:
                return k
        return None

    rows = []
    for k in keys_to_show:
        if k in (_norm("FINANCE"), _norm("BUYING DIFF GBP")):
            continue

        display = k
        value = 0.0
        source = "—"

        mkey = find_orig(manual_missing, k)
        if mkey is not None:
            display = mkey
            value = float(manual_missing[mkey] or 0.0)
            source = "Manual"
        else:
            ckey = find_orig(computed, k)
            if ckey is not None:
                display = ckey
                value = float(computed[ckey] or 0.0)
                source = "Computed"
            else:
                r = cost_map.get(k)
                if r is None:
                    value = 0.0
                    source = "Unknown item"
                else:
                    display = str(r.get("COST ITEM", k)).strip()
                    typ = str(r.get("TYPE", "")).strip().lower()
                    val = r.get("VALUE", np.nan)
                    if pd.isna(val):
                        value = 0.0
                        source = "Missing in cost_items.xlsx"
                    else:
                        val = float(val)
                        if typ == "percent":
                            value = (val / 100.0) * price_gbp
                            source = f"{val:.4f}% of price"
                        else:
                            value = val
                            source = "Fixed (cost_items.xlsx)"

        included = "YES" if k in inc_set else "NO"
        applied = value if included == "YES" else 0.0

        rows.append({
            "Cost Item": display,
            "Value GBP/t": round(value, 2),
            "Included?": included,
            "Applied GBP/t": round(applied, 2),
            "Source": source,
        })

    df = pd.DataFrame(rows)
    total_applied = float(df["Applied GBP/t"].sum()) if not df.empty else 0.0
    return df, total_applied

# =========================
# APP UI
# =========================
st.set_page_config(layout="wide")
st.title("🧮 Incoterm Auto Calculator — Result = Price − Costs + Diff (ICE live)")

left, right = st.columns([0.60, 0.40], gap="large")

with right:
    if st.button("♻️ Reload Excel (clear cache)"):
        st.cache_data.clear()
        st.rerun()

# Load freight early for POL/POD lists
freight_df_for_ui = None
freight_file_error = None
try:
    freight_df_for_ui = load_freight_table(FREIGHT_XLSX)
except Exception as e:
    freight_file_error = str(e)

with left:
    st.subheader("Inputs")
    incoterm = st.selectbox("Incoterm", INCOTERMS, index=2)
    volume = st.number_input("Volume (tons)", min_value=1, value=1, step=1)

    st.markdown("### ICE London contract (optional)")
    use_ice = st.toggle("Use ICE London futures (LIVE)", value=False, disabled=not _ice_ok())
    if not _ice_ok():
        st.caption("ICE not configured. Set ICE_XTICK_URL / ICE_USERNAME / ICE_PASSWORD.")

    ice_month_name = st.selectbox("Delivery month", [n for n, _ in COCOA_DELIVERY_MONTHS], index=0)
    ice_year_full = st.number_input("Delivery year (YYYY)", min_value=2024, max_value=2035, value=datetime.now().year, step=1)
    ice_month_code = dict(COCOA_DELIVERY_MONTHS)[ice_month_name]
    yy = ice_year_full % 100
    ice_symbol = f"C {yy:02d}{ice_month_code}-ICE"
    st.caption(f"Selected: {ice_symbol}")

    st.markdown("### Price (single number)")
    price_ccy = st.selectbox("Price currency", ["GBP", "EUR", "USD"], index=0, disabled=use_ice)
    price_input = st.number_input(
        f"Price ({'GBP' if use_ice else price_ccy}/t)",
        min_value=0.0,
        value=7500.0,
        step=10.0,
        format="%.2f",
        disabled=use_ice,
    )

    st.markdown("### Diff (adds to result)")
    diff = st.number_input("Diff (GBP/ton)", value=0.0, step=1.0, format="%.2f")

    st.markdown("---")
    st.subheader("Route / Logistics")
    container_size = st.selectbox("Container", ["20", "40"], index=1)

    if freight_df_for_ui is not None:
        fdf_c = freight_df_for_ui[freight_df_for_ui["CONTAINER"] == str(container_size)].copy()
        pol_list = sorted(fdf_c["POL"].dropna().unique().tolist())
        if pol_list:
            pol = st.selectbox("POL (from freight file)", pol_list, index=0)
            pod_list = sorted(fdf_c.loc[fdf_c["POL"] == _norm(pol), "POD"].dropna().unique().tolist())
            if pod_list:
                pod = st.selectbox("POD (from freight file)", pod_list, index=0)
            else:
                st.warning("No POD for selected POL/container in freight file. Using manual POD list.")
                pol = st.selectbox("POL (manual)", sorted(pol_options))
                pod = st.selectbox("POD (manual)", sorted(destination_options))
        else:
            st.warning("Freight file has no POL for this container. Using manual lists.")
            pol = st.selectbox("POL (manual)", sorted(pol_options))
            pod = st.selectbox("POD (manual)", sorted(destination_options))
    else:
        st.caption(f"Freight file not usable for POL/POD dropdown: {freight_file_error}")
        pol = st.selectbox("POL (manual)", sorted(pol_options))
        pod = st.selectbox("POD (manual)", sorted(destination_options))

    st.markdown("---")
    st.subheader("Warehouse")
    rent_months = st.number_input("Rent months (multiplies WAREHOUSE RENT)", min_value=0, value=1, step=1)

    st.markdown("---")
    st.subheader("Financing")
    payment_days = st.number_input("Payment terms (days)", min_value=0, value=30, step=1)
    annual_rate_pct = st.number_input("Annual financing rate (%)", min_value=0.0, value=8.5, step=0.5, format="%.2f")
    annual_rate = annual_rate_pct / 100.0

with right:
    st.subheader("Live context")
    st.caption(f"FX: EUR→GBP {eur_gbp_rate:.4f} | USD→GBP {usd_gbp_rate:.4f}")
    st.caption(f"Time: {datetime.now(ZoneInfo('Europe/Zurich')).strftime('%Y-%m-%d %H:%M:%S')}")

# =========================
# PRICE (ICE override)
# =========================
ice_used = False
if use_ice:
    try:
        last = fetch_ice_last_close(ice_symbol)
        if last is None:
            right.warning(f"No ICE value for {ice_symbol}; using manual Price.")
            price_gbp = to_gbp(price_input, price_ccy)
        else:
            ice_used = True
            price_gbp = float(last)
            right.success(f"{ice_symbol}: £{price_gbp:,.2f}/t (ICE live)")
    except Exception as e:
        right.error(f"ICE fetch failed: {e}. Using manual Price.")
        price_gbp = to_gbp(price_input, price_ccy)
else:
    price_gbp = to_gbp(price_input, price_ccy)

# =========================
# Load matrix + cost items
# =========================
cost_df, mat_df = load_cost_tables()
inc_keys = included_keys(mat_df, incoterm)

diff_applies = (_norm("BUYING DIFF GBP") in inc_keys)
diff_used = diff if diff_applies else 0.0

# =========================
# COMPUTED COSTS
# =========================
computed: dict[str, float] = {}

# --- Freight: require shipping line dropdown on the lane ---
freight_cost = 0.0
freight_details = {}

try:
    fdf = load_freight_table(FREIGHT_XLSX)
    lane = fdf[
        (fdf["POL"] == _norm(pol)) &
        (fdf["POD"] == _norm(pod)) &
        (fdf["CONTAINER"] == str(container_size))
    ].copy()

    right.caption(f"Freight lane rows found: {len(lane)}")

    if lane.empty:
        right.warning("No freight lane match → manual input")
        freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    else:
        lines = sorted(lane["SHIPPING LINE"].unique().tolist())
        chosen_line = right.selectbox("Shipping line (required)", lines, index=0)

        val, details = freight_gbp_per_ton(fdf, pol, pod, container_size, chosen_line)
        if val is None:
            right.warning("Selected shipping line has no row → manual input")
            freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        else:
            freight_cost = float(val)
            freight_details = details
            right.success(f"Freight: £{freight_cost:,.2f}/t ({chosen_line})")

        with right.expander("🔎 Freight components (per container)", expanded=False):
            if details:
                right.write(f"Currency: {details['currency']}  |  FX→GBP: {details['conv_to_gbp']:.4f}")
                right.write(f"FREIGHT: {details['freight_container']}")
                right.write(f"LINER: {details['liner_container']}")
                right.write(f"SURCHARGE: {details['surcharge_container']}")
                right.write(f"TOTAL/container: {details['total_container']}")
                right.write(f"TOTAL GBP/container: {details['total_gbp_container']:.2f}")
                right.write(f"Tons/container: {details['tons_per_container']}")
            else:
                right.info("No details available.")

        with right.expander("🔎 Freight lane rows (debug)", expanded=False):
            right.dataframe(
                lane[["SHIPPING LINE", "FREIGHT", "LINER", "SURCHARGE", "CURRENCY", "CONTAINER"]],
                use_container_width=True
            )

except Exception as e:
    right.warning(f"Freight failed: {e} → manual input")
    freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")

computed["FREIGHT"] = float(freight_cost)

# --- Warehouse computed line items ---
try:
    wh_df = load_warehouse_table(WAREHOUSE_XLSX)
    wh_name = left.selectbox("Warehouse name", sorted(wh_df.columns), index=0)
    ws = warehouse_series(wh_df, wh_name, rent_months)
    with right.expander("📦 Warehouse breakdown", expanded=False):
        right.dataframe(ws.round(2).to_frame("GBP/ton"), use_container_width=True)
        right.write(f"Total: £{float(ws.sum()):,.2f}/t")
    for k, v in ws.items():
        computed[k] = float(v)
except Exception as e:
    right.warning(f"Warehouse not loaded: {e}")
    wh_manual = right.number_input("Warehouse total manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    computed["WAREHOUSE TOTAL"] = float(wh_manual)

# =========================
# Manual inputs for missing INCLUDED items
# =========================
manual_missing_vals: dict[str, float] = {}
cost_map = {r["KEY"]: r for _, r in cost_df.iterrows()}
computed_keys = {_norm(k) for k in computed.keys()}

missing_manual_names: list[str] = []
for k in inc_keys:
    if k in computed_keys:
        continue
    if k in (_norm("FINANCE"), _norm("BUYING DIFF GBP")):
        continue
    r = cost_map.get(k)
    if r is None or pd.isna(r.get("VALUE", np.nan)):
        missing_manual_names.append(str(r.get("COST ITEM", k)).strip() if r is not None else k)

if missing_manual_names:
    right.markdown("### Manual inputs (only what can't be automated)")
    for item in missing_manual_names:
        manual_missing_vals[item] = right.number_input(
            f"{item} (GBP/ton)",
            min_value=0.0,
            value=0.0,
            step=1.0,
            format="%.2f",
            key=f"man_{_norm(item)}",
        )

# =========================
# One unified table + applied total
# =========================
cost_table, costs_subtotal_applied = build_unified_cost_table(
    inc_keys=inc_keys,
    price_gbp=price_gbp,
    cost_df=cost_df,
    computed=computed,
    manual_missing=manual_missing_vals,
)

# Finance
finance_included = (_norm("FINANCE") in inc_keys)
pre_finance_cost = costs_subtotal_applied
if finance_included and payment_days > 0 and annual_rate > 0:
    finance_cost = (annual_rate / 365.0) * float(payment_days) * pre_finance_cost
else:
    finance_cost = 0.0

total_cost = costs_subtotal_applied + finance_cost

# Result
result_per_ton = price_gbp - total_cost + diff_used
total_result = result_per_ton * float(volume)

# =========================
# OUTPUT
# =========================
right.markdown("## Results")
right.info(f"Price (GBP): **£{price_gbp:,.2f}/t**" + (f"  (ICE: {ice_symbol})" if ice_used else ""))
right.info(f"Total costs (applied): **£{total_cost:,.2f}/t**")
right.caption(f"Diff applies by matrix? {'YES' if diff_applies else 'NO'} → using £{diff_used:,.2f}/t")
right.success(f"Result per ton = Price − Costs + Diff: **£{result_per_ton:,.2f}/t**")
right.success(f"Total result (× {int(volume)} t): **£{total_result:,.2f}**")

with st.expander("📊 All costs (one table)", expanded=True):
    st.dataframe(
        cost_table.sort_values(["Included?", "Applied GBP/t"], ascending=[False, False]),
        use_container_width=True
    )
    st.caption("‘Applied GBP/t’ is what is actually counted in Total Costs, based on the Incoterm matrix.")
