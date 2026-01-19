# app.py — Incoterm Auto Calculator + ICE London LIVE (xtick)
# Excel files required:
#  - cost_items.xlsx
#  - incoterm_matrix.xlsx
#  - logistics_freight_trade_calc.xlsx
#  - warehouse_costs.xlsx
#
# Env vars for ICE xtick:
#  - ICE_XTICK_URL
#  - ICE_USERNAME
#  - ICE_PASSWORD

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

# ---------------------------
# CONFIG
# ---------------------------
BASE_CCY = "GBP"
BASE_SYMBOL = "£"

COST_ITEMS_XLSX = "cost_items.xlsx"
INCOTERM_MATRIX_XLSX = "incoterm_matrix.xlsx"
FREIGHT_XLSX = "logistics_freight_trade_calc.xlsx"
WAREHOUSE_XLSX = "warehouse_costs.xlsx"

INCOTERMS = ["EXW", "FCA", "FOB", "CFR", "CIF", "DAP", "DDP"]
RENT_ALIASES = {"WAREHOUSE RENT", "RENT", "STORAGE RENT"}  # multiplied by months

# ICE London cocoa contract months (as you had)
COCOA_DELIVERY_MONTHS = [("Mar", "H"), ("May", "K"), ("Jul", "N"), ("Sep", "U"), ("Dec", "Z")]

# ICE xtick credentials
ICE_XTICK_URL = os.getenv("ICE_XTICK_URL", "")
ICE_USERNAME = os.getenv("ICE_USERNAME", "")
ICE_PASSWORD = os.getenv("ICE_PASSWORD", "")

def _ice_ok() -> bool:
    return bool(ICE_XTICK_URL and ICE_USERNAME and ICE_PASSWORD)

# ---------------------------
# STREAMLIT PAGE
# ---------------------------
st.set_page_config(layout="wide")
st.title("🧮 Trade Margin Calculator — Incoterm Auto Mode (with ICE London LIVE)")

left, right = st.columns([0.60, 0.40], gap="large")

# ---------------------------
# HELPERS
# ---------------------------
def _norm_key(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

def _norm_port(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

def _norm_carrier(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

@st.cache_data(show_spinner=False, ttl=300)
def get_fx_rate(pair: str) -> float | None:
    try:
        t = yf.Ticker(pair)
        data = t.history(period="1d")
        if not data.empty:
            return float(data["Close"].iloc[-1])
    except Exception:
        return None
    return None

# FX (with fallbacks)
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

# ---------------------------
# ICE London LIVE (xtick)
# ---------------------------
@st.cache_data(ttl=15, show_spinner=False)
def fetch_ice_last_close(symbol: str) -> float | None:
    """
    Fetch latest 5-min bar close for a given ICE symbol via xtick.
    Returns close as float or None.
    """
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

    # entitlement / errors sometimes come back as XML with status
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

# ---------------------------
# LOAD TABLES
# ---------------------------
@st.cache_data(show_spinner=False)
def load_cost_tables():
    cost_df = pd.read_excel(COST_ITEMS_XLSX)
    mat_df = pd.read_excel(INCOTERM_MATRIX_XLSX)

    cost_df.columns = [str(c).strip().upper() for c in cost_df.columns]
    mat_df.columns = [str(c).strip().upper() for c in mat_df.columns]

    cost_df["COST_ITEM_KEY"] = cost_df["COST ITEM"].map(_norm_key)
    mat_df["COST_ITEM_KEY"] = mat_df["COST ITEM"].map(_norm_key)

    cost_df["VALUE"] = pd.to_numeric(cost_df.get("VALUE"), errors="coerce")
    cost_df["TYPE"] = cost_df.get("TYPE", "").astype(str).str.strip().str.lower()
    cost_df["TYPE"] = cost_df["TYPE"].replace({"percentage": "percent", "%": "percent"})

    for ic in INCOTERMS:
        if ic not in mat_df.columns:
            mat_df[ic] = 0
        mat_df[ic] = pd.to_numeric(mat_df[ic], errors="coerce").fillna(0).astype(int)

    return cost_df, mat_df

def included_items_for_incoterm(mat_df: pd.DataFrame, incoterm: str) -> list[str]:
    ic = incoterm.strip().upper()
    keys = mat_df.loc[mat_df[ic] == 1, "COST_ITEM_KEY"].tolist()
    return list(dict.fromkeys(keys))  # unique keep order

@st.cache_data(show_spinner=False)
def load_freight_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [str(c).strip().upper() for c in df.columns]

    needed = {"POL", "POD", "CONTAINER", "SHIPPING LINE", "ALL_IN", "CURRENCY"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Freight file missing columns: {missing}")

    df["POL"] = df["POL"].map(_norm_port)
    df["POD"] = df["POD"].map(_norm_port)
    df["CONTAINER"] = df["CONTAINER"].astype(str).str.strip()
    df["SHIPPING LINE"] = df["SHIPPING LINE"].map(_norm_carrier)
    df["CURRENCY"] = df["CURRENCY"].astype(str).str.strip().str.upper()
    df["ALL_IN"] = pd.to_numeric(df["ALL_IN"], errors="coerce")
    df = df.dropna(subset=["POL", "POD", "CONTAINER", "SHIPPING LINE", "ALL_IN", "CURRENCY"]).copy()

    # optional validity filter
    if "VALID" in df.columns:
        df["VALID_DT"] = pd.to_datetime(df["VALID"], errors="coerce", dayfirst=True)
        today = pd.Timestamp(datetime.now().date())
        df = df[(df["VALID_DT"].isna()) | (df["VALID_DT"] >= today)].copy()

    return df

def get_freight_gbp_per_ton(
    df: pd.DataFrame,
    *,
    pol: str,
    pod: str,
    container: str,
    selected_carrier: str | None,
    auto_mode: str,  # "priciest" or "cheapest"
) -> tuple[float | None, str]:
    pol_n = _norm_port(pol)
    pod_n = _norm_port(pod)
    cont = str(container).strip()

    sub = df[(df["POL"] == pol_n) & (df["POD"] == pod_n) & (df["CONTAINER"] == cont)].copy()
    if sub.empty:
        return None, "No lane match"

    def row_to_gbp(r):
        ccy = r["CURRENCY"]
        x = float(r["ALL_IN"])
        if ccy == "EUR":
            return x * eur_gbp_rate
        if ccy == "USD":
            return x * usd_gbp_rate
        return x

    sub["ALL_IN_GBP"] = sub.apply(row_to_gbp, axis=1)

    chosen = None
    if selected_carrier:
        sc = _norm_carrier(selected_carrier)
        sub_sc = sub[sub["SHIPPING LINE"] == sc]
        if not sub_sc.empty:
            chosen = sub_sc.loc[sub_sc["ALL_IN_GBP"].idxmin()] if auto_mode == "cheapest" else sub_sc.loc[sub_sc["ALL_IN_GBP"].idxmax()]
            label = f"{chosen['SHIPPING LINE']} (selected)"
        else:
            chosen = None  # fallback to auto
    if chosen is None:
        chosen = sub.loc[sub["ALL_IN_GBP"].idxmin()] if auto_mode == "cheapest" else sub.loc[sub["ALL_IN_GBP"].idxmax()]
        label = f"{chosen['SHIPPING LINE']} (auto {auto_mode})"

    per_container = float(chosen["ALL_IN_GBP"])
    tons_per_container = 20.0 if cont == "20" else 40.0
    return round(per_container / tons_per_container, 2), label

@st.cache_data(show_spinner=False)
def load_warehouse_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, index_col=0)
    df.index = df.index.map(_norm_key)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def calc_warehouse_series(wh_df: pd.DataFrame, selected_warehouse: str, rent_months: int) -> pd.Series:
    if selected_warehouse not in wh_df.columns:
        raise ValueError(f"Warehouse '{selected_warehouse}' not found in {WAREHOUSE_XLSX}")
    s = pd.to_numeric(wh_df[selected_warehouse], errors="coerce").fillna(0.0).astype(float)
    s.index = s.index.map(_norm_key)

    rent_key = next((k for k in RENT_ALIASES if k in s.index), None)
    if rent_key:
        s.loc[rent_key] = float(s.loc[rent_key]) * int(rent_months)
    return s

# ---------------------------
# AUTO COST ENGINE
# ---------------------------
def calc_auto_costs_for_incoterm(
    *,
    incoterm: str,
    base_buy: float,          # GBP
    buying_diff: float,       # GBP (manual always)
    included_keys: list[str],
    cost_df: pd.DataFrame,
    extra_computed: dict[str, float],
):
    buying_diff_included = (_norm_key("BUYING DIFF GBP") in included_keys)
    base_buy_incl = base_buy + (buying_diff if buying_diff_included else 0.0)

    cost_map = {r["COST_ITEM_KEY"]: r for _, r in cost_df.iterrows()}

    rows = []
    missing_manual_items = []

    computed_key_set = {_norm_key(k) for k in extra_computed.keys()}

    for key in included_keys:
        if key in computed_key_set:
            continue
        if key == _norm_key("FINANCE"):
            continue
        if key == _norm_key("BUYING DIFF GBP"):
            continue

        r = cost_map.get(key)
        if r is None:
            missing_manual_items.append(key)
            continue

        name = str(r.get("COST ITEM", key)).strip()
        typ = str(r.get("TYPE", "")).strip().lower()
        val = r.get("VALUE", np.nan)

        if pd.isna(val):
            missing_manual_items.append(name)
            continue

        val = float(val)
        if typ == "percent":
            amount = (val / 100.0) * base_buy
            rows.append({"Cost Item": name, "GBP/ton": round(amount, 2), "Source": f"{val:.4f}% of base buy"})
        else:
            rows.append({"Cost Item": name, "GBP/ton": round(val, 2), "Source": "Fixed from cost_items.xlsx"})

    for k, v in extra_computed.items():
        if _norm_key(k) in included_keys:
            rows.append({"Cost Item": k, "GBP/ton": round(float(v or 0.0), 2), "Source": "Computed"})

    df = pd.DataFrame(rows)
    subtotal = float(df["GBP/ton"].sum()) if not df.empty else 0.0
    return df.sort_values("Cost Item"), subtotal, base_buy_incl, missing_manual_items

# ---------------------------
# INPUTS UI
# ---------------------------
with left:
    st.subheader("Inputs")
    incoterm = st.selectbox("Incoterm", INCOTERMS, index=2)

    volume = st.number_input("Volume (tons)", min_value=1, value=1, step=1)

    st.markdown("### ICE London contract (optional)")
    use_ice_london = st.toggle(
        "Use ICE London futures (LIVE from ICE xtick)",
        value=False,
        help="If enabled, Buy price is pulled from ICE and treated as GBP/ton.",
        disabled=not _ice_ok(),
    )
    if not _ice_ok():
        st.caption("ICE xtick not configured (missing ICE_XTICK_URL / ICE_USERNAME / ICE_PASSWORD).")

    ice_month_name = st.selectbox("Delivery month", [n for n, _ in COCOA_DELIVERY_MONTHS], index=0)
    ice_year_full = st.number_input("Delivery year (YYYY)", min_value=2024, max_value=2035, value=datetime.now().year, step=1)
    ice_month_code = dict(COCOA_DELIVERY_MONTHS)[ice_month_name]
    yy = ice_year_full % 100
    ice_symbol = f"C {yy:02d}{ice_month_code}-ICE"
    st.caption(f"Selected: {ice_symbol}")

    st.markdown("### Buy price")
    buy_ccy = st.selectbox("Buy currency", ["GBP", "EUR", "USD"], index=0, disabled=use_ice_london)
    buy_price_in = st.number_input(
        f"Buy price ({'GBP' if use_ice_london else buy_ccy}/ton)",
        min_value=0.0,
        value=4000.0,
        step=10.0,
        format="%.2f",
        disabled=use_ice_london,
    )

    # Buying Diff manual (deal-specific)
    buying_diff = st.number_input("Buying Diff (GBP/ton) — manual", value=0.0, step=1.0, format="%.2f")

    st.markdown("### Sell price")
    sell_ccy = st.selectbox("Sell currency", ["GBP", "EUR", "USD"], index=0)
    sell_price_in = st.number_input(f"Sell price ({sell_ccy}/ton)", min_value=0.0, value=8500.0, step=10.0, format="%.2f")

    st.markdown("---")
    st.subheader("Route / Logistics")
    pol = st.text_input("POL (Port of Loading)", value="ABIDJAN")
    pod = st.text_input("POD (Destination)", value="ANTWERP")
    container_size = st.selectbox("Container", ["20", "40"], index=1)

    st.markdown("---")
    st.subheader("Warehouse")
    rent_months = st.number_input("Rent months (multiplies WAREHOUSE RENT)", min_value=0, value=1, step=1)

    st.markdown("---")
    st.subheader("Financing (auto only if matrix includes FINANCE)")
    payment_days = st.number_input("Payment terms (days)", min_value=0, value=30, step=1)
    annual_rate_pct = st.number_input("Annual financing rate (%)", min_value=0.0, value=8.5, step=0.5, format="%.2f")
    annual_rate = annual_rate_pct / 100.0

with right:
    st.subheader("Auto calculation")
    st.caption(f"FX (live): EUR→GBP {eur_gbp_rate:.4f} | USD→GBP {usd_gbp_rate:.4f}")
    st.caption(f"Time: {datetime.now(ZoneInfo('Europe/Zurich')).strftime('%Y-%m-%d %H:%M:%S')}")

# ---------------------------
# ICE BUY PRICE OVERRIDE
# ---------------------------
ice_used = False
ice_price_gbp = None

base_buy = None
if use_ice_london:
    try:
        ice_price_gbp = fetch_ice_last_close(ice_symbol)
        if ice_price_gbp is None:
            with right:
                st.warning(f"No ICE value for '{ice_symbol}' (no data / not entitled). Using manual Buy Price.")
            base_buy = to_gbp(buy_price_in, buy_ccy)
        else:
            ice_used = True
            base_buy = float(ice_price_gbp)  # already GBP/t
            with right:
                st.success(f"{ice_symbol}: £{base_buy:,.2f}/t (ICE live)")
    except Exception as e:
        with right:
            st.error(f"ICE fetch failed: {e}. Using manual Buy Price.")
        base_buy = to_gbp(buy_price_in, buy_ccy)
else:
    base_buy = to_gbp(buy_price_in, buy_ccy)

sell_price = to_gbp(sell_price_in, sell_ccy)

# ---------------------------
# LOAD COST + MATRIX TABLES
# ---------------------------
try:
    cost_df, mat_df = load_cost_tables()
except Exception as e:
    st.error(f"Failed to load cost tables: {e}")
    st.stop()

included_keys = included_items_for_incoterm(mat_df, incoterm)

# ---------------------------
# FREIGHT (auto if included; manual only if not found)
# ---------------------------
freight_included = (_norm_key("FREIGHT") in included_keys)
freight_per_ton = 0.0
freight_source = "Not included"
carrier_label = ""

freight_df = None
freight_load_error = None
try:
    freight_df = load_freight_table(FREIGHT_XLSX)
except Exception as e:
    freight_load_error = str(e)

with right:
    if freight_included:
        st.markdown("### Freight")
        if freight_df is None:
            st.warning(f"Freight file unavailable: {freight_load_error}")
            freight_per_ton = st.number_input("Freight manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            freight_source = "Manual (no file)"
        else:
            lane = freight_df[
                (freight_df["POL"] == _norm_port(pol)) &
                (freight_df["POD"] == _norm_port(pod)) &
                (freight_df["CONTAINER"] == str(container_size))
            ]
            carriers = sorted(lane["SHIPPING LINE"].unique().tolist()) if not lane.empty else []
            carrier_choice = st.selectbox(
                "Shipping line",
                ["Auto (priciest)", "Auto (cheapest)"] + carriers,
                index=0
            )
            auto_mode = "priciest"
            selected_carrier = None
            if carrier_choice == "Auto (cheapest)":
                auto_mode = "cheapest"
            elif carrier_choice.startswith("Auto"):
                auto_mode = "priciest"
            else:
                selected_carrier = carrier_choice

            val, label = get_freight_gbp_per_ton(
                freight_df,
                pol=pol,
                pod=pod,
                container=container_size,
                selected_carrier=selected_carrier,
                auto_mode=auto_mode,
            )
            if val is None:
                st.warning("No freight match for this POL/POD/container — please enter manually.")
                freight_per_ton = st.number_input("Freight manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
                freight_source = "Manual (no lane match)"
            else:
                freight_per_ton = float(val)
                carrier_label = label
                freight_source = f"Auto: {carrier_label}"
            st.caption(f"{freight_source} → £{freight_per_ton:,.2f}/t")

# ---------------------------
# WAREHOUSE (auto from file; fallback manual total)
# NOTE: We inject warehouse line items, so matrix can control them IF you add those rows to incoterm_matrix.xlsx.
# ---------------------------
warehouse_series = pd.Series(dtype=float)
warehouse_total_manual = 0.0

wh_df = None
wh_error = None
try:
    wh_df = load_warehouse_table(WAREHOUSE_XLSX)
except Exception as e:
    wh_error = str(e)

selected_warehouse = None
if wh_df is not None:
    with left:
        selected_warehouse = st.selectbox("Warehouse name", sorted(wh_df.columns), index=0)
    warehouse_series = calc_warehouse_series(wh_df, selected_warehouse, rent_months)
else:
    with right:
        st.warning(f"Warehouse file unavailable: {wh_error}")
        warehouse_total_manual = st.number_input("Warehouse total manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")

with right:
    if wh_df is not None:
        with st.expander("📦 Warehouse breakdown", expanded=False):
            st.write(f"Warehouse: **{selected_warehouse}**")
            st.dataframe(warehouse_series.round(2).to_frame("GBP/ton"), use_container_width=True)
            st.write(f"Total (all lines): **£{float(warehouse_series.sum()):,.2f}/t**")

# ---------------------------
# BUILD COMPUTED COSTS (auto sources)
# ---------------------------
computed_costs: dict[str, float] = {}

# Freight as computed (only included if matrix has FREIGHT=1)
computed_costs["FREIGHT"] = float(freight_per_ton)

# Warehouse: inject line items, so matrix can include/exclude each line if you add them as rows.
# If you DON'T add them to the matrix, they simply won't be included anywhere.
for k, v in warehouse_series.items():
    computed_costs[k] = float(v)

# If warehouse file missing and you still want warehouse cost, you can model it as one matrix item e.g. "WAREHOUSE TOTAL"
# then add "WAREHOUSE TOTAL" row to matrix and use manual total:
if wh_df is None and warehouse_total_manual > 0:
    computed_costs["WAREHOUSE TOTAL"] = float(warehouse_total_manual)

# ---------------------------
# AUTO COSTS FROM cost_items + percent, plus computed costs
# Anything included but blank/missing becomes manual inputs automatically
# ---------------------------
auto_df, auto_subtotal, base_buy_incl, missing_items = calc_auto_costs_for_incoterm(
    incoterm=incoterm,
    base_buy=base_buy,
    buying_diff=buying_diff,
    included_keys=included_keys,
    cost_df=cost_df,
    extra_computed=computed_costs,
)

manual_missing_costs = {}
with right:
    if missing_items:
        st.markdown("### Manual inputs (only what can't be automated)")
        for item in missing_items:
            label = str(item)
            manual_missing_costs[label] = st.number_input(
                f"{label} (GBP/ton)",
                min_value=0.0,
                value=0.0,
                step=1.0,
                format="%.2f",
                key=f"manual_{_norm_key(label)}",
            )

# add manual missing rows to breakdown
if manual_missing_costs:
    add_rows = [{"Cost Item": k, "GBP/ton": round(float(v), 2), "Source": "Manual (missing/blank)"} for k, v in manual_missing_costs.items()]
    if not auto_df.empty:
        auto_df = pd.concat([auto_df, pd.DataFrame(add_rows)], ignore_index=True)
    else:
        auto_df = pd.DataFrame(add_rows)
    auto_subtotal = float(auto_df["GBP/ton"].sum()) if not auto_df.empty else 0.0

# ---------------------------
# FINANCE (only if matrix includes FINANCE)
# ---------------------------
finance_included = (_norm_key("FINANCE") in included_keys)

pre_finance_cost = base_buy_incl + auto_subtotal

if finance_included and payment_days > 0 and annual_rate > 0:
    financing_per_ton = (annual_rate / 365.0) * float(payment_days) * pre_finance_cost
else:
    financing_per_ton = 0.0

cost_per_ton = pre_finance_cost + financing_per_ton

# ---------------------------
# RESULTS
# ---------------------------
margin_per_ton = sell_price - cost_per_ton
total_margin = margin_per_ton * float(volume)
margin_pct = (margin_per_ton / sell_price * 100.0) if sell_price > 0 else 0.0

with right:
    st.markdown("## Results")
    if ice_used:
        st.success(f"Buy (ICE live): £{base_buy:,.2f}/t — {ice_symbol}")
    else:
        st.info(f"Buy (manual, normalized): £{base_buy:,.2f}/t (entered {buy_ccy} {buy_price_in:,.2f}/t)")

    st.success(f"Sell (normalized): £{sell_price:,.2f}/t (entered {sell_ccy} {sell_price_in:,.2f}/t)")

    st.caption(f"Buying diff included by matrix? {'YES' if _norm_key('BUYING DIFF GBP') in included_keys else 'NO'}")
    st.write(f"Base buy incl diff: **£{base_buy_incl:,.2f}/t**")

    st.write(f"Auto costs subtotal (incl computed + manual missing): **£{auto_subtotal:,.2f}/t**")
    st.write(f"Finance included by matrix? **{'YES' if finance_included else 'NO'}** → £{financing_per_ton:,.2f}/t")

    st.success(f"Total landed cost: **£{cost_per_ton:,.2f}/t**")
    st.success(f"Margin per ton: **£{margin_per_ton:,.2f}/t**")
    st.success(f"Total margin: **£{total_margin:,.2f}**")
    st.caption(f"Margin % of sell: {margin_pct:.2f}%")

with st.expander("📊 Cost breakdown (what the Incoterm included)", expanded=True):
    st.write(f"Incoterm: **{incoterm}**")
    if auto_df.empty:
        st.info("No costs included for this Incoterm (or matrix rows not matching).")
    else:
        st.dataframe(auto_df.sort_values("GBP/ton", ascending=False), use_container_width=True)

with st.expander("🔎 Included items (matrix keys)", expanded=False):
    st.write(included_keys)
