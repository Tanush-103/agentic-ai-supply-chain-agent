
import pandas as pd

RATES_PATH = "Hot_order_agent/data/shipping_rates.csv"

def _normalize_priority(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return str(val).strip().lower()
    except Exception:
        return ""

def calculate_expedite_cost(order_row, dc, status):
    qty = int(order_row.get("qty", 0))
    priority = _normalize_priority(order_row.get("priority"))

    rates = pd.read_csv(RATES_PATH)
    row = rates[rates["dc"] == dc]
    if row.empty:
        base = 5.0
        expedite_mult = 1.5
    else:
        base = float(row.iloc[0]["base_rate_per_unit"])
        expedite_mult = float(row.iloc[0]["expedite_multiplier"])

    cost = base * qty if status == "OK" else base * expedite_mult * qty
    if priority == "high":
        cost *= 1.1
    return round(cost, 2)
