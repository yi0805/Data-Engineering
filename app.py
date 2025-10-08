import streamlit as st
import pandas as pd

st.title("Finance Transaction Report")

dataTransaction = pd.read_csv(r"D:\airflow material\data\raw_finance_transactions_daily.csv")
dateGl = pd.read_csv(r"D:\airflow material\data\gl_account_hierarchy.csv")
dateCost = pd.read_csv(r"D:\airflow material\data\cost_center_hierarchy.csv")
dateProfit = pd.read_csv(r"D:\airflow material\data\profit_center_hierarchy.csv")

dateGl["gl_code"] = (
    dateGl["gl_code"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
)


with st.sidebar:
    st.header("Filter")

    sel_gl = st.multiselect("GL Accounts", sorted(dateGl.get("gl_code")))
    sel_cc = st.multiselect("Cost Centers", sorted(dateCost.get("node_code")))
    sel_pc = st.multiselect("Profit Centers", sorted(dateProfit.get("node_code")))

f = dataTransaction.copy()

f["gl_account_code"] = f["gl_account_code"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

if sel_gl:
    f = f[f["gl_account_code"].isin(sel_gl)]
if sel_cc:
    f = f[f["cost_center_code"].isin(sel_cc)]
if sel_pc:
    f = f[f["profit_center_code"].isin(sel_pc)]


c1, c2, c3 = st.columns(3)

c1.metric("Transactions", f"{len(f):,}")

total_amount = f["amount"].sum()
c2.metric("Total Amount", f"{total_amount:,.0f}")

avg_amount = total_amount / len(f) 
c3.metric("Amount per day", f"{avg_amount:,.0f}")

st.subheader("Top 10 GL Amount")
top_gl = (
        f.groupby("gl_account_code")["amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
)
st.bar_chart(top_gl)

st.download_button(
    "Download the filtered CSV",
    data=f.to_csv(index=False),
    file_name="filtered_transactions.csv",
    mime="text/csv",
    disabled=f.empty,  
)