import streamlit as st
import subprocess
import sys
import os
import pandas as pd
from src.producers.ai_agent_data_synthesizer import run_synthetic_production_agent
from src.schema_enforcer_agent.ai_agent import build_consumer_agent

st.set_page_config(layout="wide", page_title="AI Data Platform Hub")
st.title("🎛️ Distributed Multi-Agent Data Platform Hub")

# SESSION STATE INITIALIZATION: Setup temporary memory buffers for live visualizations
if "temp_payload" not in st.session_state:
    st.session_state.temp_payload = None
if "temp_mode" not in st.session_state:
    st.session_state.temp_mode = None
# 💥 NEW STATE TRACKER: Preserves your Ollama Compliance text across page reruns
if "audit_report" not in st.session_state:
    st.session_state.audit_report = None

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
    
    selected_mode = st.selectbox(
        "Source Data Format Choice",
        ["Graph (Neo4j)", "Key-Value (Redis)", "Document (MongoDB)"]
    )
    
    if st.button("✨ Synthesize & Route Event"):
        with st.spinner(f"Agent 1 generating custom {selected_mode} payload..."):            
            result = run_synthetic_production_agent(selected_mode)
            
            if result["status"] == "success":
                st.success(f"🚀 Dispatched {result['mode']} variant model cleanly.")
                
                # BUFFER UPDATE: Cache the raw LLM payload and format token choice in memory
                st.session_state.temp_payload = result.get("raw_generated_json", None)
                st.session_state.temp_mode = selected_mode
                
                # 💥 DYNAMIC CLEAR: Wipe the previous audit report when generating brand new data
                st.session_state.audit_report = None
            else:
                st.error(f"❌ Generation failed: {result['message']}")

    # DYNAMIC TEMPORARY LIVE INGESTION PREVIEW BLOCK
    if st.session_state.temp_payload and st.session_state.temp_mode:
        st.markdown("#### 👁️ Temporary Live Ingestion Preview")
        
        # 1. RENDER GRAPH VARIANT
        if st.session_state.temp_mode == "Graph (Neo4j)":
            st.info("🎯 **Graph Database Object Structure (Nodes & Relationship Links)**")
            st.code(f"(:User {{id: '{st.session_state.temp_payload.get('payload',{}).get('start',{}).get('id','Unknown')}'}})"
                    f" -[:TRANSACTED {{amount: {st.session_state.temp_payload.get('payload',{}).get('properties',{}).get('amount',0.0)}}}]-> "
                    f"(:Merchant {{id: '{st.session_state.temp_payload.get('payload',{}).get('end',{}).get('id','Unknown')}'}})", language="cypher")
            with st.expander("View Raw Neo4j CDC JSON"):
                st.json(st.session_state.temp_payload)
                
        # 2. RENDER DOCUMENT VARIANT
        elif st.session_state.temp_mode == "Document (MongoDB)":
            st.warning("📄 **Hierarchical BSON Document Structure (Nested Schemas)**")
            st.json(st.session_state.temp_payload)
            
        # 3. RENDER KEY-VALUE VARIANT
        else:
            st.success("🔑 **Flat Key-Value Cache Map Structure (Fast Hashing Lookups)**")
            v_col1, v_col2, v_col3 = st.columns(3)
            with v_col1:
                st.metric("CACHE_KEY (user_id)", st.session_state.temp_payload.get("user_id", "Unknown"))
            with v_col2:
                st.metric("VAL (merchant_id)", st.session_state.temp_payload.get("merchant_id", "Unknown"))
            with v_col3:
                st.metric("VAL (amount)", f"${st.session_state.temp_payload.get('amount', 0.0)}")

    st.markdown("---")
    st.markdown("#### Agent 2: Manual Consumer Override")
    if st.button("🤖 Run Consumer AI Agent Loop"):
        with st.spinner("Local Agent 2 processing landing zone metrics..."):
            
            # STATE RESET FLUSH: Clear out input visualization displays immediately on Agent 2 click
            st.session_state.temp_payload = None
            st.session_state.temp_mode = None
            
            # Execute LangGraph clean compliance mappings
            graph = build_consumer_agent()
            output = graph.invoke({"raw_records": [], "log_report": ""})
            
            # 💥 STATE CAPTURE: Assign the log_report string safely to a session state register 
            st.session_state.audit_report = output.get("log_report", "No report text compiled.")
            st.rerun()

    # 💥 DYNAMIC AUDIT DISCOVERY WINDOW: Renders the Ollama assessment securely across layout updates
    if st.session_state.audit_report:
        st.markdown("#### 🛡️ Compliance Inspection Log Output")
        st.info(st.session_state.audit_report)


with col2:
    st.subheader("📊 Lake Storage Visualizations")
    BASE_DIR = os.getcwd()
    STRUCTURED_HISTORY_FILE = os.path.join(BASE_DIR, "historical_data_lake", "structured_history.csv")
    
    try:
        if os.path.exists(STRUCTURED_HISTORY_FILE):
            df_hist = pd.read_csv(STRUCTURED_HISTORY_FILE)
            st.markdown("#### 🏆 Curated Analytics Stratum (`structured_history.csv`)")
            st.dataframe(df_hist.tail(15), use_container_width=True)
        else:
            st.info("📂 The `structured_history.csv` data lake file hasn't been created on disk yet. Run Agent 2 to populate it.")
    except Exception as view_err:
        st.error(f"Failed to load display logs: {view_err}")

    if st.button("🔄 Sync Display Channels"):
        st.rerun()
