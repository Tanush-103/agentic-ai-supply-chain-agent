
import os
import streamlit as st
import pandas as pd
from hot_order_agent_core import hoa

st.set_page_config(page_title="Hot Order Agent", layout="wide")
st.title("🔥 Hot Order Agent Dashboard")

default_orders_path = "Hot_order_agent/data/sample_orders.csv"
#default_orders_path = "Hot_order_agent\data\sample_orders.csv"
orders_df = pd.read_csv(default_orders_path)

with st.expander("📦 Current Orders (from data/sample_orders.csv)"):
    st.dataframe(orders_df, use_container_width=True)

st.subheader("Upload Orders CSV (optional)")
uploaded = st.file_uploader(
    "CSV with columns: order_id,product,qty,customer,priority,origin,destination[,customer_email]",
    type=["csv"]
)
if uploaded is not None:
    try:
        orders_df = pd.read_csv(uploaded)
        st.success("Uploaded orders loaded.")
    except Exception as e:
        st.error(f"Failed to read uploaded CSV: {e}")

col1, col2 = st.columns(2)
with col1:
    if st.button("▶️ Run Hot Order Agent"):
        results = hoa.process_orders(orders_df)
        st.session_state["hoa_results"] = results
        st.success("Orders processed!")

with col2:
    if st.button("🔁 Re-run on sample data"):
        orders_df = pd.read_csv(default_orders_path)
        results = hoa.process_orders(orders_df)
        st.session_state["hoa_results"] = results
        st.success("Re-run complete.")

st.markdown("---")
st.subheader("Results")
results_df = st.session_state.get("hoa_results")
if results_df is not None and not results_df.empty:
    st.dataframe(results_df, use_container_width=True)
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Orders", len(results_df))
    k2.metric("At-Risk Orders", int((results_df["status"] == "At-Risk").sum()))
    k3.metric("Avg Expedite $", round(results_df["expedite_cost"].mean(), 2))

st.markdown("---")
st.subheader("Customer Communication Log")
#log_path = "logs/communication.log"
log_path = "Hot_order_agent/logs/communication.log"
if not os.path.exists(log_path):
    open(log_path, "w").write("")
with open(log_path, "r") as f:
    st.text(f.read())
