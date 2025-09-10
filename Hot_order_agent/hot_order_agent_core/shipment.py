
import pandas as pd

RATES_PATH = "data/shipping_rates.csv"

def _normalize_priority(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return str(val).strip().lower()
    except Exception:
        return ""

def estimate_shipment_days(order_row, dc, status):
    priority = _normalize_priority(order_row.get("priority"))
    rates = pd.read_csv(RATES_PATH)
    row = rates[rates["dc"] == dc]
    if row.empty:
        base_days = 5
        expedite_days = 2
    else:
        base_days = int(row.iloc[0]["base_days"])
        expedite_days = int(row.iloc[0]["expedite_days"])

    days = base_days if status == "OK" else expedite_days
    if priority == "high":
        days = max(1, days - 1)
    return days
