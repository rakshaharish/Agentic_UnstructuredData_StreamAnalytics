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
        with st.spinner("Local Agent 2 processing landing zone metrics..."):
            # Import your clean LangGraph code directly
            from src.schema_enforcer_agent.ai_agent import build_consumer_agent
            
            graph = build_consumer_agent()
            output = graph.invoke({"raw_records": [], "log_report": ""})
            st.info(output["log_report"])


with col2:
    st.subheader("📊 Lake Storage Visualizations")
    # Define the absolute path to your active structured history data lake file
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    STRUCTURED_HISTORY_FILE = os.path.join(BASE_DIR, "historical_data_lake", "structured_history.csv")
    
    # 💥 DATA LAKE DISK FETCH FIX: Read straight from your clean master log file instead of SQL
    try:
        if os.path.exists(STRUCTURED_HISTORY_FILE):
            df_hist = pd.read_csv(STRUCTURED_HISTORY_FILE)
            st.markdown("#### 🏆 Curated Analytics Stratum (`structured_history.csv`)")
            st.dataframe(df_hist.tail(15), use_container_width=True) # Displays your top 15 newest rows
        else:
            st.info("📂 The `structured_history.csv` data lake file hasn't been created on disk yet. Run Agent 2 to populate it.")
    except Exception as view_err:
        st.error(f"Failed to load display logs: {view_err}")

    if st.button("🔄 Sync Display Channels"):
        st.rerun()