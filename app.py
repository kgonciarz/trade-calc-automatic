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
# YOUR DROPDOWNS (paste your full lists here)
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
    return list(dict.fromkeys(keys))  # unique keep order

# =========================
# FREIGHT TABLE
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

    # optional validity filter
    if "VALID" in df.columns:
        df["VALID_DT"] = pd.to_datetime(df["VALID"], errors="coerce", dayfirst=True)
        today = pd.Timestamp(datetime.now().date())
        df = df[(df["VALID_DT"].isna()) | (df["VALID_DT"] >= today)].copy()

    return df

def freight_gbp_per_ton(
    df: pd.DataFrame,
    pol: str,
    pod: str,
    container: str,
    carrier_choice: str,
) -> tuple[float | None, str]:
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
# WAREHOUSE TABLE
# =========================
@st.cache_data(show_spinner=False)
def load_warehouse_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, index_col=0)
    df.index = df.index.map(_norm)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def warehouse_series(wh_df: pd.DataFrame, wh_name: str, rent_months: int) -> pd.Series:
    if wh_name not in wh_df.columns:
        raise ValueError(f"Warehouse '{wh_name}' not found in {WAREHOUSE_XLSX}")

    s = pd.to_numeric(wh_df[wh_name], errors="coerce").fillna(0.0).astype(float)
    s.index = s.index.map(_norm)

    rent_key = next((k for k in RENT_ALIASES if k in s.index), None)
    if rent_key:
        s.loc[rent_key] = float(s.loc[rent_key]) * int(rent_months)
    return s

# =========================
# AUTO COST ENGINE
# =========================
def calc_cost_breakdown(
    inc_keys: list[str],
    base_buy_gbp: float,
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
            amt = (val / 100.0) * base_buy_gbp
            rows.append({"Cost Item": name, "GBP/ton": round(amt, 2), "Source": f"{val:.4f}% of buy"})
        else:
            rows.append({"Cost Item": name, "GBP/ton": round(val, 2), "Source": "Fixed (cost_items.xlsx)"})

    # add computed items if included
    for name, v in computed.items():
        if _norm(name) in inc_keys:
            rows.append({"Cost Item": name, "GBP/ton": round(float(v or 0.0), 2), "Source": "Computed"})

    df = pd.DataFrame(rows)
    subtotal = float(df["GBP/ton"].sum()) if not df.empty else 0.0
    return df, subtotal, missing_manual

# =========================
# UI
# =========================
st.set_page_config(layout="wide")
st.title("🧮 Cocoa Trade Assistant — Incoterm Auto Calculator (ICE live)")

left, right = st.columns([0.60, 0.40], gap="large")

with left:
    st.subheader("Trade Parameters")

    incoterm = st.selectbox("Incoterm", INCOTERMS, index=2)
    volume = st.number_input("Volume (tons)", min_value=1, value=1, step=1)

    st.markdown("### ICE London contract")
    use_ice = st.toggle("Use ICE London futures (LIVE from ICE xtick)", value=False, disabled=not _ice_ok())
    if not _ice_ok():
        st.caption("ICE xtick not configured. Set ICE_XTICK_URL / ICE_USERNAME / ICE_PASSWORD.")

    ice_month_name = st.selectbox("Delivery month", [n for n, _ in COCOA_DELIVERY_MONTHS], index=0)
    ice_year_full = st.number_input("Delivery year (YYYY)", min_value=2024, max_value=2035, value=datetime.now().year, step=1)
    ice_month_code = dict(COCOA_DELIVERY_MONTHS)[ice_month_name]
    yy = ice_year_full % 100
    ice_symbol = f"C {yy:02d}{ice_month_code}-ICE"
    st.caption(f"Selected: {ice_symbol}")

    st.markdown("### Buy price")
    buy_ccy = st.selectbox("Buy currency", ["GBP", "EUR", "USD"], index=0, disabled=use_ice)
    buy_price_input = st.number_input(
        f"Buy price ({'GBP' if use_ice else buy_ccy}/t)",
        min_value=0.0,
        value=7500.0,
        step=10.0,
        format="%.2f",
        disabled=use_ice,
    )

    st.markdown("### Selling price")
    sell_ccy = st.selectbox("Sell currency", ["GBP", "EUR", "USD"], index=0)
    sell_price_input = st.number_input(f"Sell price ({sell_ccy}/t)", min_value=0.0, value=8500.0, step=10.0, format="%.2f")

    st.markdown("### Buying Diff (manual, adds to margin/revenue)")
    buying_diff = st.number_input("Buying Diff (GBP/ton)", value=0.0, step=1.0, format="%.2f")

    st.markdown("---")
    st.subheader("Route / Logistics")
    port = st.selectbox("Port of Loading (POL)", sorted(pol_options))
    destination = st.selectbox("Destination (POD)", sorted(destination_options))
    container_size = st.selectbox("Container size", ["20", "40"], index=1)

    st.markdown("---")
    st.subheader("Warehouse")
    rent_months = st.number_input("Warehouse rent months", min_value=0, value=1, step=1)

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
# BUY PRICE (ICE override)
# =========================
ice_used = False
if use_ice:
    try:
        last = fetch_ice_last_close(ice_symbol)
        if last is None:
            st.warning(f"No ICE value for {ice_symbol}; using manual buy price.")
            base_buy = to_gbp(buy_price_input, buy_ccy)
        else:
            ice_used = True
            base_buy = float(last)  # GBP/t
            with right:
                st.success(f"{ice_symbol}: £{base_buy:,.2f}/t (ICE live)")
    except Exception as e:
        st.error(f"ICE fetch failed: {e}. Using manual buy price.")
        base_buy = to_gbp(buy_price_input, buy_ccy)
else:
    base_buy = to_gbp(buy_price_input, buy_ccy)

sell_price = to_gbp(sell_price_input, sell_ccy)

# =========================
# LOAD TABLES
# =========================
cost_df, mat_df = load_cost_tables()
keys_inc = included_keys(mat_df, incoterm)

# =========================
# FREIGHT AUTO (only if included)
# =========================
freight_cost = 0.0
freight_note = "Not included"
if _norm("FREIGHT") in keys_inc:
    try:
        freight_df = load_freight_table(FREIGHT_XLSX)

        lane = freight_df[
            (freight_df["POL"] == _norm(port)) &
            (freight_df["POD"] == _norm(destination)) &
            (freight_df["CONTAINER"] == str(container_size))
        ]
        carriers = sorted(lane["SHIPPING LINE"].unique().tolist()) if not lane.empty else []
        carrier_choice = right.selectbox(
            "Shipping line",
            ["Auto (priciest)", "Auto (cheapest)"] + carriers,
            index=0,
        )

        val, label = freight_gbp_per_ton(freight_df, port, destination, container_size, carrier_choice)
        if val is None:
            right.warning("No freight match for this lane → enter manually.")
            freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            freight_note = "Manual (no lane match)"
        else:
            freight_cost = float(val)
            freight_note = f"Auto ({label})"
        right.caption(f"{freight_note} → £{freight_cost:,.2f}/t")

    except Exception as e:
        right.warning(f"Freight auto failed: {e} → enter manually.")
        freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        freight_note = "Manual (error)"

# =========================
# WAREHOUSE AUTO (line items)
# (You should add those warehouse row names to incoterm_matrix if you want incoterm control.)
# =========================
computed = {}
computed["FREIGHT"] = freight_cost

warehouse_total = 0.0
try:
    wh_df = load_warehouse_table(WAREHOUSE_XLSX)
    wh_name = left.selectbox("Warehouse name", sorted(wh_df.columns), index=0)
    wh_s = warehouse_series(wh_df, wh_name, rent_months)
    warehouse_total = float(wh_s.sum())

    with right.expander("📦 Warehouse breakdown", expanded=False):
        right.write(f"Warehouse: **{wh_name}**")
        right.dataframe(wh_s.round(2).to_frame("GBP/ton"), use_container_width=True)
        right.write(f"Total (all lines): **£{warehouse_total:,.2f}/t**")

    # inject each line item as computed so matrix can include them if present
    for k, v in wh_s.items():
        computed[k] = float(v)

except Exception as e:
    right.warning(f"Warehouse file not loaded ({e}). Using manual warehouse total.")
    warehouse_total = right.number_input("Warehouse total manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    computed["WAREHOUSE TOTAL"] = float(warehouse_total)

# =========================
# AUTO COSTS + MANUAL for missing items
# =========================
auto_df, auto_subtotal, missing_manual = calc_cost_breakdown(
    inc_keys=keys_inc,
    base_buy_gbp=base_buy,
    cost_df=cost_df,
    computed=computed,
)

manual_missing_costs = {}
if missing_manual:
    right.markdown("### Manual inputs (only what cannot be automated)")
    for item in missing_manual:
        label = str(item)
        manual_missing_costs[label] = right.number_input(
            f"{label} (GBP/ton)",
            min_value=0.0,
            value=0.0,
            step=1.0,
            format="%.2f",
            key=f"man_{_norm(label)}",
        )

if manual_missing_costs:
    add = pd.DataFrame([{"Cost Item": k, "GBP/ton": float(v), "Source": "Manual"} for k, v in manual_missing_costs.items()])
    auto_df = pd.concat([auto_df, add], ignore_index=True) if not auto_df.empty else add
    auto_subtotal = float(auto_df["GBP/ton"].sum()) if not auto_df.empty else 0.0

# =========================
# FINANCE (only if included)
# =========================
finance_included = (_norm("FINANCE") in keys_inc)

pre_finance_cost = base_buy + auto_subtotal  # IMPORTANT: buying diff is NOT in cost
if finance_included and payment_days > 0 and annual_rate > 0:
    financing = (annual_rate / 365.0) * float(payment_days) * pre_finance_cost
else:
    financing = 0.0

landed_cost = pre_finance_cost + financing

# =========================
# BUYING DIFF APPLIES TO REVENUE / MARGIN
# controlled by matrix row BUYING DIFF GBP
# =========================
diff_applies = (_norm("BUYING DIFF GBP") in keys_inc)
effective_sell = sell_price + (buying_diff if diff_applies else 0.0)

margin_per_ton = effective_sell - landed_cost
total_margin = margin_per_ton * float(volume)
margin_pct = (margin_per_ton / effective_sell * 100.0) if effective_sell > 0 else 0.0

# =========================
# OUTPUT
# =========================
right.markdown("## Results")
if ice_used:
    right.success(f"Buy (ICE live): £{base_buy:,.2f}/t — {ice_symbol}")
else:
    right.info(f"Buy (normalized): £{base_buy:,.2f}/t")

right.success(f"Sell (normalized): £{sell_price:,.2f}/t")
right.caption(f"Buying diff applies by matrix? {'YES' if diff_applies else 'NO'}")
right.info(f"Effective sell (sell + diff if applies): **£{effective_sell:,.2f}/t**")

right.write(f"Auto costs subtotal: **£{auto_subtotal:,.2f}/t**")
right.write(f"Finance included by matrix? **{'YES' if finance_included else 'NO'}** → £{financing:,.2f}/t")
right.success(f"Total landed cost: **£{landed_cost:,.2f}/t**")

right.success(f"Margin per ton: **£{margin_per_ton:,.2f}/t**")
right.success(f"Total margin: **£{total_margin:,.2f}**")
right.caption(f"Margin %: {margin_pct:.2f}%")

with st.expander("📊 Cost breakdown (incoterm included)", expanded=True):
    st.write(f"Incoterm: **{incoterm}**")
    if auto_df.empty:
        st.info("No costs included (or matrix/cost names do not match).")
    else:
        st.dataframe(auto_df.sort_values("GBP/ton", ascending=False), use_container_width=True)

with st.expander("🔎 Included items (matrix keys)", expanded=False):
    st.write(keys_inc)
