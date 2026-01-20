# app.py — Incoterm Auto Calculator (single "Price") + ICE London LIVE
# Result = Price − TotalCosts + Diff
# Freight: user chooses POL, POD, CONTAINER, SHIPPING LINE (from the lane),
# and freight per container = FREIGHT + LINER + SURCHARGE (handles "not included"),
# then converted to GBP and divided by tons/container.
#
# Excel files required in same folder:
#   - cost_items.xlsx
#   - incoterm_matrix.xlsx
#   - logistics_freight_trade_calc.xlsx
#   - warehouse_costs.xlsx
#
# Env vars:
#   ICE_XTICK_URL, ICE_USERNAME, ICE_PASSWORD

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
TRANSPORT_XLSX = "Transport.xlsx"
DEFAULT_TRUCK_TONS = 25.0

RENT_ALIASES = {"WAREHOUSE RENT", "RENT", "STORAGE RENT"}
COCOA_DELIVERY_MONTHS = [("Mar", "H"), ("May", "K"), ("Jul", "N"), ("Sep", "U"), ("Dec", "Z")]

ICE_XTICK_URL = os.getenv("ICE_XTICK_URL", "")
ICE_USERNAME  = os.getenv("ICE_USERNAME", "")
ICE_PASSWORD  = os.getenv("ICE_PASSWORD", "")

def _ice_ok() -> bool:
    return bool(ICE_XTICK_URL and ICE_USERNAME and ICE_PASSWORD)


# =========================
# HELPERS
# =========================
def _norm(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

def _norm_col(c: str) -> str:
    return re.sub(r"\s+", " ", str(c or "")).strip().upper()

def _norm_container(x) -> str:
    """
    Normalize container values like 20, 20.0, "20 ", "40FT" -> "20"/"40"
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    # try numeric first
    try:
        f = float(s)
        if abs(f - 20.0) < 0.01:
            return "20"
        if abs(f - 40.0) < 0.01:
            return "40"
        return str(int(round(f)))
    except Exception:
        pass
    m = re.search(r"(20|40)", s)
    return m.group(1) if m else s

def _money_to_float(x) -> float:
    """
    Robust parsing for cells like:
      250
      "250"
      "$250.00"
      "not included"
      "1,234.56"
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    s = str(x).strip().lower()
    if not s or "not included" in s:
        return 0.0
    s = re.sub(r"[^0-9\-\.,]", "", s)
    s = s.replace(",", "")
    try:
        return float(s) if s else 0.0
    except Exception:
        return 0.0


# =========================
# FX
# =========================
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

    cost_df.columns = [_norm_col(c) for c in cost_df.columns]
    mat_df.columns  = [_norm_col(c) for c in mat_df.columns]

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

@st.cache_data(show_spinner=False)
def load_transport_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [_norm_col(c) for c in df.columns]

    needed = {"POL", "POD", "SERVICE PROVIDER", "RATE"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Transport file missing columns: {missing}")

    df["POL"] = df["POL"].map(_norm)
    df["POD"] = df["POD"].map(_norm)
    df["SERVICE PROVIDER"] = df["SERVICE PROVIDER"].astype(str).str.strip()
    df["RATE"] = df["RATE"].apply(_money_to_float)  # handles "2,400.00"

    df = df.dropna(subset=["POL", "POD", "SERVICE PROVIDER", "RATE"]).copy()
    return df



def transport_gbp_per_ton(
    tdf: pd.DataFrame,
    pol: str,
    pod: str,
    provider: str,
    eur_gbp_rate: float,
    tons_per_truck: float = DEFAULT_TRUCK_TONS,
) -> float | None:
    pol_n = _norm(pol)
    pod_n = _norm(pod)

    sub = tdf[(tdf["POL"] == pol_n) & (tdf["POD"] == pod_n) & (tdf["SERVICE PROVIDER"] == provider)].copy()
    if sub.empty:
        return None

    # pick cheapest rate for that provider (if duplicates)
    rate_eur = float(sub["RATE"].min())
    rate_gbp = rate_eur * float(eur_gbp_rate)
    return round(rate_gbp / float(tons_per_truck), 2)

MARINE_INS_XLSX = "Marine_insurance.xlsx"

@st.cache_data(show_spinner=False)
def load_marine_insurance_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [_norm_col(c) for c in df.columns]

    needed = {"MARINE INSURANCE", "VALUE", "TYPE"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Marine insurance file missing columns: {missing}")

    df["MARINE INSURANCE"] = df["MARINE INSURANCE"].astype(str).str.strip()
    df["TYPE"] = df["TYPE"].astype(str).str.strip().str.lower().replace({"percentage": "percent", "%": "percent"})
    df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce")

    df = df.dropna(subset=["MARINE INSURANCE", "VALUE"]).copy()
    return df

# =========================
# FREIGHT - SIMPLIFIED AND FIXED
# =========================
@st.cache_data(show_spinner=False)
def load_freight_table(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [_norm_col(c) for c in df.columns]

    # Check for required columns
    needed = {"POL", "POD", "CONTAINER", "SHIPPING LINE", "FREIGHT", "CURRENCY"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Freight file missing columns: {missing}")

    # Add optional columns if missing
    if "LINER" not in df.columns:
        df["LINER"] = 0
    if "SURCHARGE" not in df.columns:
        df["SURCHARGE"] = 0

    # Clean up the data - KEEP IT SIMPLE
    df["POL_CLEAN"] = df["POL"].astype(str).str.strip().str.upper()
    df["POD_CLEAN"] = df["POD"].astype(str).str.strip().str.upper()
    df["CONTAINER_CLEAN"] = df["CONTAINER"].apply(_norm_container)
    df["SHIPPING_LINE_CLEAN"] = df["SHIPPING LINE"].astype(str).str.strip().str.upper()
    df["CURRENCY"] = df["CURRENCY"].astype(str).str.strip().str.upper()

    # Convert monetary values
    df["FREIGHT"] = df["FREIGHT"].apply(_money_to_float)
    df["LINER"] = df["LINER"].apply(_money_to_float)
    df["SURCHARGE"] = df["SURCHARGE"].apply(_money_to_float)

    # Filter out bad rows
    df = df[
        (df["POL_CLEAN"] != "") & 
        (df["POD_CLEAN"] != "") & 
        (df["CONTAINER_CLEAN"].isin(["20", "40"])) &
        (df["SHIPPING_LINE_CLEAN"] != "")
    ].copy()

    return df

def freight_gbp_per_ton(
    df: pd.DataFrame,
    pol: str,
    pod: str,
    container: str,
    shipping_line: str,
) -> tuple[float | None, dict]:
    
    # Normalize inputs
    pol_clean = str(pol).strip().upper()
    pod_clean = str(pod).strip().upper()
    cont_clean = _norm_container(container)
    sl_clean = str(shipping_line).strip().upper()

    # Filter for matching lane
    lane = df[
        (df["POL_CLEAN"] == pol_clean) &
        (df["POD_CLEAN"] == pod_clean) &
        (df["CONTAINER_CLEAN"] == cont_clean) &
        (df["SHIPPING_LINE_CLEAN"] == sl_clean)
    ].copy()

    if lane.empty:
        return None, {}

    # Calculate total per container
    lane["TOTAL_CONTAINER"] = lane["FREIGHT"] + lane["LINER"] + lane["SURCHARGE"]
    chosen = lane.iloc[0]  # Take first match

    ccy = str(chosen["CURRENCY"]).upper()
    conv = eur_gbp_rate if ccy == "EUR" else usd_gbp_rate if ccy == "USD" else 1.0

    total_container = float(chosen["TOTAL_CONTAINER"])
    total_gbp_container = total_container * conv

    tons = 20.0 if cont_clean == "20" else 40.0
    gbp_ton = round(total_gbp_container / tons, 2)

    details = {
        "currency": ccy,
        "conv_to_gbp": conv,
        "freight_container": float(chosen["FREIGHT"]),
        "liner_container": float(chosen["LINER"]),
        "surcharge_container": float(chosen["SURCHARGE"]),
        "total_container": total_container,
        "total_gbp_container": total_gbp_container,
        "tons_per_container": tons,
        "container": cont_clean,
        "shipping_line": sl_clean,
    }
    return gbp_ton, details


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

    # Filter out FINANCE and BUYING DIFF GBP from keys to show (diff is handled separately in result calculation)
    keys_to_show = list(dict.fromkeys(
        [k for k in inc_keys if k not in (_norm("FINANCE"), _norm("BUYING DIFF GBP"), _norm("BASED DIFF"))]
        + [_norm(k) for k in computed.keys()]
        + [_norm(k) for k in manual_missing.keys() if _norm(k) not in (_norm("BUYING DIFF GBP"), _norm("BASED DIFF"))]
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

with right:
    st.subheader("Live context")
    st.caption(f"FX: EUR→GBP {eur_gbp_rate:.4f} | USD→GBP {usd_gbp_rate:.4f}")
    st.caption(f"Time: {datetime.now(ZoneInfo('Europe/Zurich')).strftime('%Y-%m-%d %H:%M:%S')}")

# Load freight early for dropdown sourcing
freight_df_for_ui = None
freight_file_error = None
try:
    freight_df_for_ui = load_freight_table(FREIGHT_XLSX)
    right.success(f"✅ Freight file loaded: {len(freight_df_for_ui)} rows")
except Exception as e:
    freight_file_error = str(e)
    right.error(f"❌ Freight file error: {freight_file_error}")

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
        value=4000.0,
        step=10.0,
        format="%.2f",
        disabled=use_ice,
    )

    st.markdown("---")
    st.subheader("Route / Logistics")
    container_size = st.selectbox("Container", ["20", "40"], index=1)

    if freight_df_for_ui is None or freight_df_for_ui.empty:
        st.warning("⚠️ Freight file not loaded - using manual inputs")
        pol = st.text_input("POL (manual)", value="")
        pod = st.text_input("POD (manual)", value="")
        shipping_line = st.text_input("Shipping line (manual)", value="")
    else:
        # Filter by selected container size
        fdf_c = freight_df_for_ui[
            freight_df_for_ui["CONTAINER_CLEAN"] == _norm_container(container_size)
        ].copy()

        # Get unique POLs
        pol_options = sorted(fdf_c["POL_CLEAN"].unique().tolist())
        pol = st.selectbox("POL", pol_options, index=0 if pol_options else 0)

        # Get PODs for selected POL
        pod_options = sorted(
            fdf_c[fdf_c["POL_CLEAN"] == pol]["POD_CLEAN"].unique().tolist()
        )
        pod = st.selectbox("POD", pod_options, index=0 if pod_options else 0)

        # Get shipping lines for selected POL+POD+Container
        lane_filtered = fdf_c[
            (fdf_c["POL_CLEAN"] == pol) & 
            (fdf_c["POD_CLEAN"] == pod)
        ].copy()

        shipping_line_options = sorted(lane_filtered["SHIPPING_LINE_CLEAN"].unique().tolist())
        
        # Check if any STS_ options exist
        sts_options = [x for x in shipping_line_options if x.startswith("STS_")]
        final_options = sts_options if sts_options else shipping_line_options

        if final_options:
            shipping_line = st.selectbox("Shipping line", final_options, index=0)
        else:
            st.warning("No shipping lines found for this route")
            shipping_line = st.text_input("Shipping line (manual)", value="")

        st.caption(f"Available shipping lines on this route: {len(final_options)}")

    st.markdown("---")
    st.subheader("Warehouse")
    rent_months = st.number_input("Rent months (multiplies WAREHOUSE RENT)", min_value=0, value=1, step=1)

    st.markdown("---")
    st.subheader("Financing")
    payment_days = st.number_input("Payment terms (days)", min_value=0, value=30, step=1)
    annual_rate_pct = st.number_input("Annual financing rate (%)", min_value=0.0, value=8.5, step=0.5, format="%.2f")
    annual_rate = annual_rate_pct / 100.0

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


# =========================
# COMPUTED COSTS
# =========================
computed: dict[str, float] = {}

# --- Freight computed from selected lane + shipping line ---
freight_cost = 0.0
freight_details = {}
try:
    fdf = load_freight_table(FREIGHT_XLSX)
    val, details = freight_gbp_per_ton(fdf, pol, pod, container_size, shipping_line)
    if val is None:
        right.warning("⚠️ No freight row for that combination → using manual input")
        freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    else:
        freight_cost = float(val)
        freight_details = details
        right.success(f"✅ Freight calculated: £{freight_cost:,.2f}/ton")
except Exception as e:
    right.error(f"Freight calculation failed: {e}")
    freight_cost = right.number_input("FREIGHT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")

computed["FREIGHT"] = float(freight_cost)

with right.expander("🚢 Freight details", expanded=False):
    right.write(f"**Freight GBP/t:** £{freight_cost:,.2f}")
    if freight_details:
        right.json(freight_details)

# --- Warehouse line items ---
try:
    wh_df = load_warehouse_table(WAREHOUSE_XLSX)
    wh_name = left.selectbox("Warehouse name", sorted(wh_df.columns), index=0)
    ws = warehouse_series(wh_df, wh_name, rent_months)
    with right.expander("📦 Warehouse breakdown", expanded=False):
        right.dataframe(ws.round(2).to_frame("GBP/ton"), use_container_width=True)
        right.write(f"**Total:** £{float(ws.sum()):,.2f}/t")
    for k, v in ws.items():
        computed[k] = float(v)
except Exception as e:
    right.warning(f"Warehouse not loaded: {e}")
    wh_manual = right.number_input("Warehouse total manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
    computed["WAREHOUSE TOTAL"] = float(wh_manual)

# =========================
# MARINE INSURANCE (only if included by incoterm)
# =========================
marine_needed = (_norm("MARINE INSURANCE (1st)") in inc_keys) or (_norm("MARINE INSURANCE (2nd)") in inc_keys)

if marine_needed:
    try:
        midf = load_marine_insurance_table(MARINE_INS_XLSX)

        opts = midf["MARINE INSURANCE"].tolist()
        # choose in right column so it feels like other “computed” selectors
        chosen_mi = right.selectbox("Marine insurance option", opts, index=0)

        row = midf.loc[midf["MARINE INSURANCE"] == chosen_mi].iloc[0]
        mi_type = str(row["TYPE"]).lower()
        mi_val = float(row["VALUE"])

        if mi_type == "percent":
            marine_cost = (mi_val / 100.0) * float(price_gbp)
            mi_source = f"{mi_val:.4f}% of price"
        else:
            marine_cost = mi_val
            mi_source = "Fixed"

        # If your matrix has both (1st) and (2nd), you can decide:
        # - apply the same selected option to whichever is included.
        if _norm("MARINE INSURANCE (1st)") in inc_keys:
            computed["MARINE INSURANCE (1st)"] = float(marine_cost)
        if _norm("MARINE INSURANCE (2nd)") in inc_keys:
            computed["MARINE INSURANCE (2nd)"] = float(marine_cost)

        right.caption(f"Marine insurance: £{marine_cost:,.2f}/t ({mi_source})")

    except Exception as e:
        right.warning(f"Marine insurance table error: {e}. Using manual input.")
        if _norm("MARINE INSURANCE (1st)") in inc_keys:
            computed["MARINE INSURANCE (1st)"] = float(
                right.number_input("MARINE INSURANCE (1st) manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            )
        if _norm("MARINE INSURANCE (2nd)") in inc_keys:
            computed["MARINE INSURANCE (2nd)"] = float(
                right.number_input("MARINE INSURANCE (2nd) manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            )

# =========================
# TRANSPORT (inland) — tick + POL/POD + Service Provider dropdown
# =========================
use_transport = left.checkbox("Add TRANSPORT (inland)?", value=False)

if use_transport:
    try:
        tdf = load_transport_table(TRANSPORT_XLSX)

        # POL dropdown from transport file
        t_pol_list = sorted(tdf["POL"].unique().tolist())
        t_pol = left.selectbox("Transport POL", t_pol_list, index=0)

        # POD depends on POL
        t_pod_list = sorted(tdf.loc[tdf["POL"] == _norm(t_pol), "POD"].unique().tolist())
        t_pod = left.selectbox("Transport POD", t_pod_list, index=0)

        # Route rows (may have multiple providers)
        route_df = tdf[(tdf["POL"] == _norm(t_pol)) & (tdf["POD"] == _norm(t_pod))].copy()
        route_df = route_df.sort_values(["SERVICE PROVIDER", "RATE"])

        if route_df.empty:
            right.warning("No transport rates for this POL/POD → manual input")
            transport_gbp_ton = right.number_input(
                "TRANSPORT manual (GBP/ton)",
                min_value=0.0, value=0.0, step=1.0, format="%.2f"
            )
            computed["TRANSPORT (inland)"] = float(transport_gbp_ton)
        else:
            # ✅ Service provider dropdown (all providers available on this route)
            providers = sorted(route_df["SERVICE PROVIDER"].unique().tolist())
            provider = left.selectbox("Service Provider", providers, index=0)

            # If multiple rows exist for same provider, pick the cheapest rate for that provider
            prov_df = route_df[route_df["SERVICE PROVIDER"] == provider].copy()
            chosen_row = prov_df.loc[prov_df["RATE"].idxmin()]
            rate_eur_truck = float(chosen_row["RATE"])

            tons_per_truck = left.number_input(
                "Tons per truck (transport ÷)",
                min_value=1.0,
                value=float(DEFAULT_TRUCK_TONS),
                step=1.0,
                format="%.0f",
            )

            rate_gbp_truck = rate_eur_truck * eur_gbp_rate
            transport_gbp_ton = round(rate_gbp_truck / float(tons_per_truck), 2)

            right.caption(
                f"Transport {t_pol} → {t_pod} ({provider}): "
                f"€{rate_eur_truck:,.2f}/truck → £{rate_gbp_truck:,.2f}/truck → "
                f"£{transport_gbp_ton:,.2f}/t (÷{tons_per_truck:.0f})"
            )

            computed["TRANSPORT (inland)"] = float(transport_gbp_ton)

            with right.expander("🔎 Transport route rows (debug)", expanded=False):
                right.dataframe(route_df[["POL","POD","SERVICE PROVIDER","RATE"]], use_container_width=True)

    except Exception as e:
        right.warning(f"Transport failed: {e} → manual input")
        computed["TRANSPORT (inland)"] = float(
            right.number_input("TRANSPORT manual (GBP/ton)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        )


# =========================
# Manual inputs for missing INCLUDED items
# =========================
manual_missing_vals: dict[str, float] = {}
cost_map = {r["KEY"]: r for _, r in cost_df.iterrows()}
computed_keys = {_norm(k) for k in computed.keys()}

missing_manual_names: list[str] = []
diff = 0.0  # Initialize diff variable
diff_item_name = None  # Track the diff item name
for k in inc_keys:
    if k in computed_keys:
        continue
    # Skip FINANCE - it's calculated separately
    if k in (_norm("FINANCE"),):
        continue
    # Handle BUYING DIFF GBP / BASED DIFF specially - show as manual input but DON'T add to costs
    if k in (_norm("BUYING DIFF GBP"), _norm("BASED DIFF")):
        r = cost_map.get(k)
        diff_item_name = str(r.get("COST ITEM", k)).strip() if r is not None else k
        continue
    r = cost_map.get(k)
    if r is None or pd.isna(r.get("VALUE", np.nan)):
        missing_manual_names.append(str(r.get("COST ITEM", k)).strip() if r is not None else k)

# =========================
# Manual inputs for missing INCLUDED items + DIFF
# =========================
manual_missing_vals: dict[str, float] = {}
cost_map = {r["KEY"]: r for _, r in cost_df.iterrows()}
computed_keys = {_norm(k) for k in computed.keys()}

missing_manual_names: list[str] = []
diff = 0.0  # Initialize diff variable
diff_item_name = None  # Track the diff item name
for k in inc_keys:
    if k in computed_keys:
        continue
    # Skip FINANCE - it's calculated separately
    if k in (_norm("FINANCE"),):
        continue
    # Handle BUYING DIFF GBP / BASED DIFF specially - show as manual input but DON'T add to costs
    if k in (_norm("BUYING DIFF GBP"), _norm("BASED DIFF")):
        r = cost_map.get(k)
        diff_item_name = str(r.get("COST ITEM", k)).strip() if r is not None else k
        continue
    r = cost_map.get(k)
    if r is None or pd.isna(r.get("VALUE", np.nan)):
        missing_manual_names.append(str(r.get("COST ITEM", k)).strip() if r is not None else k)

if missing_manual_names or diff_item_name:
    right.markdown("### Manual inputs")
    
    # Show diff input first if applicable
    if diff_item_name:
        diff = right.number_input(
            f"{diff_item_name} (GBP/ton) - ADDS TO MARGIN",
            min_value=-1000.0,
            value=0.0,
            step=1.0,
            format="%.2f",
            key="diff_input",
            help="This value is ADDED to the result (not subtracted as a cost)"
        )
    
    # Then show other manual inputs
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

# Check if diff applies based on incoterm matrix
diff_applies = (_norm("BUYING DIFF GBP") in inc_keys or _norm("BASED DIFF") in inc_keys)
diff_used = diff if diff_applies else 0.0

# Result
result_per_ton = price_gbp - total_cost + diff_used
total_result = result_per_ton * float(volume)

# =========================
# OUTPUT
# =========================
right.markdown("## Results")
right.info(f"**Price:** £{price_gbp:,.2f}/t" + (f" (ICE: {ice_symbol})" if ice_used else ""))
right.info(f"**Total costs (applied):** £{total_cost:,.2f}/t")
if diff != 0.0:
    right.success(f"**Diff (added to margin):** £{diff:,.2f}/t")
right.success(f"**Result per ton = Price − Costs + Diff:** £{result_per_ton:,.2f}/t")
right.success(f"**Total result (× {int(volume)} t):** £{total_result:,.2f}")

with st.expander("📊 All costs (one table)", expanded=True):
    st.dataframe(
        cost_table.sort_values(["Included?", "Applied GBP/t"], ascending=[False, False]),
        use_container_width=True
    )
    st.caption("'Applied GBP/t' is what is counted in Total Costs based on the Incoterm matrix.")

with st.expander("🔎 Included items (matrix keys)", expanded=False):
    st.write(inc_keys)