import streamlit as st
import subprocess
import sys
import sqlite3
import os
import pandas as pd
from src.producers.ai_agent_data_synthesizer import run_synthetic_production_agent
from src.schema_enforcer_agent.ai_agent import build_consumer_agent

st.set_page_config(layout="wide", page_title="AI Data Platform Hub")
st.title("🎛️ Distributed Multi-Agent Data Platform Hub")

openai_key = st.sidebar.text_input("OpenAI API Key", type="password")
if openai_key:
    os.environ["OPENAI_API_KEY"] = openai_key

col1, col2 = st.columns(2)

with col1:
    st.subheader("🛠️ Operations Dashboard")
    if st.button("🚀 Boot Infrastructure Setup"):
        subprocess.Popen(["docker-compose", "up", "-d"])
        st.success("Docker streaming components active!")

    if st.button("⚡ Execute Spark Stream Pipeline"):
        subprocess.Popen([sys.executable, "-m", "src.streaming.spark_transformer"])
        st.success("Spark continuous transformer active!")

    st.markdown("---")
    st.markdown("#### Agent 1: Synthetic Factory Generation")
    if st.button("✨ Synthesize & Route Event"):
        # if not openai_key: 
        #    st.error("🔑 OpenAI API Key required in the sidebar.")
        # else:
        with st.spinner("AI Agent generating and streaming polymorphic payloads..."):
            result = run_synthetic_production_agent() # (openai_key)
            if result["status"] == "success":
                st.success(f"🚀 Agent 1 successfully dispatched transactional variants for user: {result['user']} (${result['amount']})")
            else:
                st.error(f"❌ Synthesis failed: {result['message']}")

    st.markdown("---")
    st.markdown("#### Agent 2: Manual Consumer Override")
    if st.button("🤖 Run Consumer AI Agent Loop"):
        if not openai_key: st.error("API Key required.")
        else:
            graph = build_consumer_agent()
            output = graph.invoke({"raw_records": [], "log_report": ""})
            st.info(output["log_report"])

with col2:
    st.subheader("📊 Lake Storage Visualizations")
    try:
        conn = sqlite3.connect('historical_warehouse.db')
        df_land = pd.read_sql_query("SELECT rowid, source_topic, landed_at FROM raw_landing_zone ORDER BY rowid DESC LIMIT 5", conn)
        st.markdown("#### Ingested Zone Entries (`raw_landing_zone`)")
        st.dataframe(df_land, use_container_width=True)
        
        df_hist = pd.read_sql_query("SELECT * FROM structured_history ORDER BY rowid DESC LIMIT 5", conn)
        st.markdown("#### Curated Analytics Stratum (`structured_history`)")
        st.dataframe(df_hist, use_container_width=True)
        conn.close()
    except:
        st.info("System storage is currently unpopulated.")

    if st.button("🔄 Sync Display Channels"):
        st.rerun()
