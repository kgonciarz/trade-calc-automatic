# app.py — Incoterm Auto Calculator (single "Price") + ICE London LIVE
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
# YOUR DROPDOWNS (same style as your original)
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
# LOAD TABLES
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
    cost_df["TYPE"] = cost_df.get("TYPE", "").astype(str).str.strip().str.lower().replace({"percentage": "percent", "%": "percent"})

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
# FREIGHT
# =========================
@st.cache_data(show_spinner=False)
def load_freight_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [str(c).strip().upper() for c in df.columns]

    needed = {"POL", "POD", "CONTAINER", "SHIPPING LINE", "ALL_IN", "CURRENCY"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Freight file missing columns: {missing}")

    df["POL"] = df["POL"].map(_norm)
    df["POD"] = df["POD"].map(_norm)
    df["CONTAINER"] = df["CONTAINER"].astype(str).str.strip()
    df["SHIPPING LINE"] = df["SHIPPING LINE"].map(_norm)
    df["CURRENCY"] = df["CURRENCY"].astype(str).str.strip().str.upper()
    df["ALL_IN"] = pd.to_numeric(df["ALL_IN"], errors="coerce")
    df = df.dropna(subset=["POL", "POD", "CONTAINER", "SHIPPING LINE", "ALL_IN", "CURRENCY"]).copy()

    if "VALID" in df.columns:
        df["VALID_DT"] = pd.to_datetime(df["VALID"], errors="coerce", dayfirst=True)
        today = pd.Timestamp(datetime.now().date())
        df = df[(df["VALID_DT"].isna()) | (df["VALID_DT"] >= today)].copy()

    return df

def freight_gbp_per_ton(df: pd.DataFrame, pol: str, pod: str, container: str, carrier_choice: str) -> tuple[float | None, str]:
    pol_n = _norm(pol)
    pod_n = _norm(pod)
    cont = str(container).strip()

    sub = df[(df["POL"] == pol_n) & (df["POD"] == pod_n) & (df["CONTAINER"] == cont)].copy()
    if sub.empty:
        return None, "No lane match"

    def row_to_gbp(r):
        x = float(r["ALL_IN"])
        ccy = r["CURRENCY"]
        if ccy == "EUR":
            return x * eur_gbp_rate
        if ccy == "USD":
            return x * usd_gbp_rate
        return x

    sub["ALL_IN_GBP"] = sub.apply(row_to_gbp, axis=1)

    if carrier_choice == "Auto (priciest)":
        chosen = sub.loc[sub["ALL_IN_GBP"].idxmax()]
        label = f"{chosen['SHIPPING LINE']} (auto priciest)"
        per_container = float(chosen["ALL_IN_GBP"])
    elif carrier_choice == "Auto (cheapest)":
        chosen = sub.loc[sub["ALL_IN_GBP"].idxmin()]
        label = f"{chosen['SHIPPING LINE']} (auto cheapest)"
        per_container = float(chosen["ALL_IN_GBP"])
    else:
        sc = _norm(carrier_choice)
        sub_sc = sub[sub["SHIPPING LINE"] == sc]
        if sub_sc.empty:
            chosen = sub.loc[sub["ALL_IN_GBP"].idxmax()]
            label = f"{chosen['SHIPPING LINE']} (fallback auto priciest)"
            per_container = float(chosen["ALL_IN_GBP"])
        else:
            chosen = sub_sc.loc[sub_sc["ALL_IN_GBP"].idxmax()]
            label = f"{chosen['SHIPPING LINE']} (selected)"
            per_container = float(chosen["ALL_IN_GBP"])

    tons_per_container = 20.0 if cont == "20" else 40.0
    return round(per_container / tons_per_container, 2), label

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
# COST ENGINE (Price-based)
# =========================
def calc_cost_breakdown(
    inc_keys: list[str],
    price_gbp: float,
    cost_df: pd.DataFrame,
    computed: dict[str, float],
):
    cost_map = {r["KEY"]: r for _, r in cost_df.iterrows()}
    computed_keys = {_norm(k) for k in computed.keys()}

    rows = []
    missing_manual = []

    for k in inc_keys:
        if k in computed_keys:
            continue
        if k in (_norm("FINANCE"), _norm("BUYING DIFF GBP")):
            continue

        r = cost_map.get(k)
        if r is None:
            missing_manual.append(k)
            continue

        name = str(r.get("COST ITEM", k)).strip()
        typ = str(r.get("TYPE", "")).strip().lower()
        val = r.get("VALUE", np.nan)

        if pd.isna(val):
            missing_manual.append(name)
            continue

        val = float(val)
        if typ == "percent":
            # percent is applied to PRICE now (your requested model)
            amt = (val / 100.0) * price_gbp
            rows.append({"Cost Item": name, "GBP/ton": round(amt, 2), "Source": f"{val:.4f}% of price"})
        else:
            rows.append({"Cost Item": name, "GBP/ton": round(val, 2), "Source": "Fixed (cost_items.xlsx)"})

    for name, v in computed.items():
        if _norm(name) in inc_keys:
            rows.append({"Cost Item": name, "GBP/ton": round(float(v or 0.0), 2), "Source": "Computed"})

    df = pd.DataFrame(rows)
    subtotal = float(df["GBP/ton"].sum()) if not df.empty else 0.0
    return df, subtotal, missing_manual

# =========================
# APP UI
# =========================
st.set_page_config(layout="wide")
st.title("🧮 Incoterm Auto Calculator — Result = Price − Costs + Diff (ICE live)")

left, right = st.columns([0.60, 0.40], gap="large")

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
    pol = st.selectbox("POL", sorted(pol_options))
    pod = st.selectbox("POD", sorted(destination_options))
    container_size = st.selectbox("Container", ["20", "40"], index=1)

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

# Diff applies only if matrix has BUYING DIFF GBP = 1
diff_applies = (_norm("BUYING DIFF GBP") in inc_keys)
diff_used = diff if diff_applies else 0.0

# =========================
# Freight (computed)
# =========================
computed = {}
freight_cost = 0.0

if _norm("FREIGHT") in inc_keys:
    try:
        fdf = load_freight_table(FREIGHT_XLSX)
        lane = fdf[(fdf["POL"] == _norm(pol)) & (fdf["POD"] == _norm(pod)) & (fdf["CONTAINER"] == str(container_size))]
        carriers = sorted(lane["SHIPPING LINE"].unique().tolist()) if not lane.empty else []
        carrier_choice = right.selectbox("Shipping line", ["Auto (priciest)", "Auto (cheapest)"] + carriers, index=0)

        val, label = freight_gbp_per_ton(fdf, pol, pod, container_size, carrier_choice)
        if val is None:
            right.warning("No freight match → manual input")
            freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        else:
            freight_cost = float(val)
            right.caption(f"Freight: {label} → £{freight_cost:,.2f}/t")
    except Exception as e:
        right.warning(f"Freight failed: {e} → manual")
        freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")

computed["FREIGHT"] = float(freight_cost)

# =========================
# Warehouse (inject line items)
# =========================
warehouse_total = 0.0
try:
    wh_df = load_warehouse_table(WAREHOUSE_XLSX)
    wh_name = left.selectbox("Warehouse name", sorted(wh_df.columns), index=0)
    ws = warehouse_series(wh_df, wh_name, rent_months)
    warehouse_total = float(ws.sum())
    with right.expander("📦 Warehouse breakdown", expanded=False):
        right.dataframe(ws.round(2).to_frame("GBP/ton"), use_container_width=True)
        right.write(f"Total: £{warehouse_total:,.2f}/t")

    for k, v in ws.items():
        computed[k] = float(v)
except Exception as e:
    right.warning(f"Warehouse not loaded: {e}")
    wh_manual = right.number_input("Warehouse total manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    computed["WAREHOUSE TOTAL"] = float(wh_manual)

# =========================
# Auto costs + manual missing
# =========================
cost_breakdown_df, costs_subtotal, missing_manual = calc_cost_breakdown(
    inc_keys=inc_keys,
    price_gbp=price_gbp,
    cost_df=cost_df,
    computed=computed,
)

manual_missing = {}
if missing_manual:
    right.markdown("### Manual inputs (only what can't be automated)")
    for item in missing_manual:
        manual_missing[str(item)] = right.number_input(
            f"{item} (GBP/ton)",
            min_value=0.0,
            value=0.0,
            step=1.0,
            format="%.2f",
            key=f"man_{_norm(item)}",
        )

if manual_missing:
    add = pd.DataFrame([{"Cost Item": k, "GBP/ton": float(v), "Source": "Manual"} for k, v in manual_missing.items()])
    cost_breakdown_df = pd.concat([cost_breakdown_df, add], ignore_index=True) if not cost_breakdown_df.empty else add
    costs_subtotal = float(cost_breakdown_df["GBP/ton"].sum()) if not cost_breakdown_df.empty else 0.0

# =========================
# Finance (optional)
# =========================
finance_included = (_norm("FINANCE") in inc_keys)
pre_finance_cost = costs_subtotal
if finance_included and payment_days > 0 and annual_rate > 0:
    finance_cost = (annual_rate / 365.0) * float(payment_days) * pre_finance_cost
else:
    finance_cost = 0.0

total_cost = costs_subtotal + finance_cost

# =========================
# RESULT = Price - Costs + Diff
# =========================
result_per_ton = price_gbp - total_cost + diff_used
total_result = result_per_ton * float(volume)

# =========================
# OUTPUT
# =========================
right.markdown("## Results")
right.info(f"Price (GBP): **£{price_gbp:,.2f}/t**" + (f"  (ICE: {ice_symbol})" if ice_used else ""))
right.info(f"Total costs: **£{total_cost:,.2f}/t**")
right.caption(f"Diff applies by matrix? {'YES' if diff_applies else 'NO'} → using £{diff_used:,.2f}/t")

right.success(f"Result per ton = Price − Costs + Diff: **£{result_per_ton:,.2f}/t**")
right.success(f"Total result (× {int(volume)} t): **£{total_result:,.2f}**")

with st.expander("📊 Cost breakdown (incoterm included)", expanded=True):
    st.write(f"Incoterm: **{incoterm}**")
    if cost_breakdown_df.empty:
        st.info("No costs included (or matrix/cost names do not match).")
    else:
        st.dataframe(cost_breakdown_df.sort_values("GBP/ton", ascending=False), use_container_width=True)

with st.expander("🔎 Included items (matrix keys)", expanded=False):
    st.write(inc_keys)
