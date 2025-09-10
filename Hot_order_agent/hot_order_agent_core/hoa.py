
import pandas as pd
from .inventory import check_inventory
from .cost import calculate_expedite_cost
from .shipment import estimate_shipment_days
from .communication import send_customer_update

ORDERS_PATH = "data/sample_orders.csv"

def _compute_row(row):
    status, dc, available_qty = check_inventory(row)
    cost = calculate_expedite_cost(row, dc, status)
    eta = estimate_shipment_days(row, dc, status)
    return status, dc, available_qty, cost, eta

def process_orders(orders: pd.DataFrame) -> pd.DataFrame:
    results = []
    for _, row in orders.iterrows():
        order_id = row.get("order_id")
        cust_name = row.get("customer", "Unknown")
        cust_email = row.get("customer_email") if "customer_email" in orders.columns else None

        status, dc, available_qty, cost, eta = _compute_row(row)
        send_customer_update(order_id, status, dc, cost, eta, available_qty, cust_name, cust_email)
        results.append({
            "order_id": order_id,
            "product": row.get("product"),
            "qty": int(row.get("qty", 0)),
            "customer": cust_name,
            "customer_email": cust_email if cust_email else "",
            "priority": row.get("priority", ""),
            "status": status,
            "selected_dc": dc,
            "available_qty": int(available_qty),
            "expedite_cost": float(cost),
            "estimated_days": int(eta)
        })
    return pd.DataFrame(results)

def process_single_order(order_id, overrides=None):
    df = pd.read_csv(ORDERS_PATH)

    # Normalize IDs
    if "order_id" in df.columns:
        df["order_id"] = df["order_id"].astype(str).str.strip()

    # Find row
    idx = df.index[df["order_id"].astype(str) == str(order_id).strip()]

    # Auto-create if missing, using overrides
    if len(idx) == 0:
        o = overrides or {}
        new_row = {
            "order_id": str(order_id).strip(),
            "product": o.get("product", "Unknown"),
            "qty": int(pd.to_numeric(o.get("qty", 0), errors="coerce") or 0),
            "customer": o.get("customer", "Unknown"),
            "priority": (str(o.get("priority", "Normal")) if o.get("priority") is not None else "Normal"),
            "origin": o.get("origin", ""),
            "destination": o.get("destination", ""),
            "customer_email": o.get("customer_email", ""),
        }
        for col in ["order_id","product","qty","customer","priority","origin","destination","customer_email"]:
            if col not in df.columns:
                df[col] = None
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        idx = df.index[df["order_id"].astype(str) == str(order_id).strip()]

    i = idx[0]

    # Apply overrides
    if overrides:
        for k, v in overrides.items():
            if k in df.columns:
                df.at[i, k] = v

    if "customer_email" not in df.columns:
        df["customer_email"] = ""

    row = df.iloc[i].copy()

    status, dc, available_qty = check_inventory(row)
    cost = calculate_expedite_cost(row, dc, status)
    eta = estimate_shipment_days(row, dc, status)

    df.to_csv(ORDERS_PATH, index=False)
    send_customer_update(
        row.get("order_id"),
        status, dc, cost, eta, available_qty,
        row.get("customer", "Unknown"),
        row.get("customer_email"),
    )

    return {
        "order_id": row.get("order_id"),
        "status": status,
        "selected_dc": dc,
        "available_qty": int(available_qty),
        "expedite_cost": float(cost),
        "estimated_days": int(eta),
    }
