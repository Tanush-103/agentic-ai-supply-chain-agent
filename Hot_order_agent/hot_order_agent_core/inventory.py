
import pandas as pd

INV_PATH = "data/inventory.csv"

def check_inventory(order_row):
    product = order_row.get("product")
    qty = int(order_row.get("qty", 0))

    inv = pd.read_csv(INV_PATH)
    rows = inv[inv["product"] == product].copy()
    if rows.empty:
        return "At-Risk", "None", 0

    rows.sort_values("available_qty", ascending=False, inplace=True)
    best = rows.iloc[0]
    available = int(best["available_qty"])

    if available >= qty:
        return "OK", best["dc"], qty
    return "At-Risk", best["dc"], available
