import os
import glob
import pandas as pd
import json
import hashlib
from datetime import datetime
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama 
from langgraph.graph import StateGraph, START, END
from src.schema_enforcer_agent.graph_state import ConsumerState

# Establish data lake directory references
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_LAKE_PATH = os.path.join(BASE_DIR, "historical_data_lake", "raw_landing_zone")
STRUCTURED_HISTORY_FILE = os.path.join(BASE_DIR, "historical_data_lake", "structured_history.csv")

def fetch_landing_zone_data(state: ConsumerState) -> ConsumerState:
    """Reads raw parquet records straight from the historical data lake directory."""
    try:
        parquet_files = glob.glob(os.path.join(RAW_LAKE_PATH, "*.parquet"))
        if not parquet_files:
            state["raw_records"] = []
            return state

        # 1. Combine raw files into a single master layout DataFrame
        df_raw = pd.concat([pd.read_parquet(fp) for fp in parquet_files])
        
        # 💥 THE UNIQUE HASH FIX: Reset index first to get a distinct incremental sequential number per row
        df_raw = df_raw.reset_index(drop=True)

        # Append the incremental loop row sequence index number into the hash encoder string 
        # to ensure every transaction has a unique hash even if timestamps are identical
        def generate_row_hash(index, row):
            combined_string = f"{index}_{row['source_topic']}_{str(row['raw_payload'])}_{str(row['landed_at'])}"
            return hashlib.md5(combined_string.encode('utf-8')).hexdigest()

        # Generate unique row signatures
        df_raw["rowid"] = [generate_row_hash(i, r) for i, r in df_raw.iterrows()]

        # 2. Incremental processing filter: Exclude entries that already exist in our history log
        if os.path.exists(STRUCTURED_HISTORY_FILE):
            df_existing = pd.read_csv(STRUCTURED_HISTORY_FILE)
            processed_ids = set(df_existing["raw_ref_id"].dropna().astype(str).tolist())
            df_raw = df_raw[~df_raw["rowid"].astype(str).isin(processed_ids)]

        # 3. Pull the top 10 unparsed entries to process through the current micro-batch
        df_target = df_raw.head(10)
        
        primitive_records = []
        for _, row in df_target.iterrows():
            primitive_records.append({
                "rowid": str(row["rowid"]),
                "source_topic": str(row["source_topic"]),
                "raw_payload": str(row["raw_payload"]),
                "landed_at": str(row["landed_at"])
            })
            
        state["raw_records"] = primitive_records
    except Exception as e:
        state["raw_records"] = []
    return state

def enforce_schema_and_write(state: ConsumerState) -> ConsumerState:
    """Enforces multi-model schemas deterministically and audits them via LLM."""
    if not state["raw_records"]:
        state["log_report"] = "No new raw data records found to process."
        return state

    cleansed_list = []
    
    # Capture the exact current timestamp for this processing batch execution loop
    current_time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    for record in state["raw_records"]:
        raw_ref_id = record["rowid"]
        source_topic = record["source_topic"]
        
        user_id, merchant_id, amount = "Unknown", "Unknown", 0.0
        
        try:
            payload = json.loads(record["raw_payload"])
            
            # 1. Parse Graph Structure (Neo4j CDC Format)
            if source_topic == "neo4j-tx-topic" or "payload" in payload:
                inner = payload.get("payload", payload)
                user_id = inner.get("start", {}).get("id", "Unknown")
                merchant_id = inner.get("end", {}).get("id", "Unknown")
                amount = float(inner.get("properties", {}).get("amount", 0.0))
                
            # 2. Parse Document Structure (MongoDB Format)
            elif source_topic == "mongo-tx-topic" or "tx_id" in payload:
                user_id = payload.get("user", {}).get("id", payload.get("user_id", "Unknown"))
                merchant_id = payload.get("merchant", {}).get("name", payload.get("merchant_id", "Unknown"))
                amount = float(payload.get("amount", 0.0))
                
            # 3. Parse Key-Value Structure (Redis Layout Format)
            else:
                user_id = payload.get("user_id", "Unknown")
                merchant_id = payload.get("merchant_id", "Unknown")
                amount = float(payload.get("amount", 0.0))
                
        except Exception:
            continue # Skip corrupted rows safely

        # 💥 TIMESTAMP FIX: Attach the structured_at timestamp parameter into your data lake rows
        cleansed_list.append({
            "raw_ref_id": raw_ref_id,
            "user_id": str(user_id),
            "merchant_id": str(merchant_id),
            "amount": float(amount),
            "structured_at": current_time_str
        })

    if not cleansed_list:
        state["log_report"] = "Failed to parse any rows cleanly."
        return state

    # Commit verified records to disk data lake storage
    df_new_history = pd.DataFrame(cleansed_list)
    if not os.path.exists(STRUCTURED_HISTORY_FILE):
        df_new_history.to_csv(STRUCTURED_HISTORY_FILE, index=False)
    else:
        df_new_history.to_csv(STRUCTURED_HISTORY_FILE, mode="a", header=False, index=False)

    # Run LLM Audit as an evaluation observer (not a writing bottleneck)
    try:
        llm = ChatOllama(model="llama3", temperature=0.1,
                         base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"))
        prompt = ChatPromptTemplate.from_messages([
            ("system", "Review this structured compliance list and summarize anomalies or high velocities."),
            ("user", "Audit these transactions: {data}")
        ])
        audit_response = llm.invoke(prompt.format(data=json.dumps(cleansed_list)))
        audit_text = f"\n\n🤖 LLM Governance Audit:\n{audit_response.content}"
    except Exception as e:
        audit_text = f"\n\n⚠️ LLM Audit skipped: {e}"

    state["log_report"] = f"Processed & structured {len(cleansed_list)} log lake streams successfully.{audit_text}"
    return state

def build_consumer_agent():
    workflow = StateGraph(ConsumerState)
    workflow.add_node("fetch_data", fetch_landing_zone_data)
    workflow.add_node("enforce_schema", enforce_schema_and_write)
    workflow.add_edge(START, "fetch_data")
    workflow.add_edge("fetch_data", "enforce_schema")
    workflow.add_edge("enforce_schema", END)
    return workflow.compile()