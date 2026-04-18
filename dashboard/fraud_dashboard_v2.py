import streamlit as st 
import pandas as pd 
 
st.title("🚨 Real-Time Fraud Detection Dashboard") 
 
df = pd.read_parquet("stream_data/realtime_output/") 
 
st.metric("Total Transaksi", len(df)) 
st.metric("Total Fraud", len(df[df["status"]=="FRAUD"])) 
 
st.dataframe(df.tail(10)) 
st.bar_chart(df["status"].value_counts()) 