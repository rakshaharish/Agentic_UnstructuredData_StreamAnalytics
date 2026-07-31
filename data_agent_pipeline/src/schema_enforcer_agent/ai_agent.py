import os
import glob
import pandas as pd
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama 
from langgraph.graph import StateGraph, START, END
from src.schema_enforcer_agent.graph_state import ConsumerState

# Establish clean data lake directory references
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_LAKE_PATH = os.path.join(BASE_DIR, "historical_data_lake", "raw_landing_zone")
STRUCTURED_HISTORY_FILE = os.path.join(BASE_DIR, "historical_data_lake", "structured_history.csv")

def fetch_landing_zone_data(state: ConsumerState) -> ConsumerState:
    """Reads raw parquet records straight from the historical data lake directory."""
    try:
        # 1. Grab all chunk parquet files generated dynamically by your Spark transformer stream
        parquet_files = glob.glob(os.path.join(RAW_LAKE_PATH, "*.parquet"))
        if not parquet_files:
            state["raw_records"] = []
            return state

        # 2. Combine files into a unified dataframe and assign a mock tracking sequence id
        df_raw = pd.concat([pd.read_parquet(fp) for fp in parquet_files])
        df_raw = df_raw.reset_index().rename(columns={"index": "rowid"})

        # 3. Incremental processing filter: Exclude entries that already exist in our history log
        if os.path.exists(STRUCTURED_HISTORY_FILE):
            df_existing = pd.read_csv(STRUCTURED_HISTORY_FILE)
            processed_ids = set(df_existing["raw_ref_id"].dropna().astype(int).tolist())
            df_raw = df_raw[~df_raw["rowid"].isin(processed_ids)]

        # 4. Pull the top 10 unparsed entries to process through the current micro-batch
        df_target = df_raw.head(10)
        state["raw_records"] = df_target.to_dict(orient="records")
    except Exception as e:
        state["raw_records"] = []
    return state

def enforce_schema_and_write(state: ConsumerState) -> ConsumerState:
    """Invokes local Ollama Llama3 model to structure fields into the history layer."""
    if not state["raw_records"]:
        state["log_report"] = "No new raw data records found to process."
        return state

    # Uses your local background Ollama endpoint to parse variables into strict structures
    llm = ChatOllama(model="llama3", temperature=0.1, format="json")
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Parse mixed credit card transactional fields into a clean flat schema: user_id, merchant_id, amount.
        Return a raw JSON array ONLY: [{{"raw_ref_id": rowid, "user_id": "str", "merchant_id": "str", "amount": float}}]
        Do not include markdown ticks or parameters outside the array string."""),
        ("user", "Extract parameters from: {records}")
    ])
    
    response = llm.invoke(prompt.format(records=json.dumps(state["raw_records"])))
    
    try:
        clean_out = response.content.strip().lstrip("```json").rstrip("```").strip()
        parsed_list = json.loads(clean_out)
        
        # 5. Convert clean structured objects list into a target dataframe append segment
        df_new_history = pd.DataFrame(parsed_list)
        
        # 6. Commit append tracking updates safely into the historical database layer storage
        if not os.path.exists(STRUCTURED_HISTORY_FILE):
            df_new_history.to_csv(STRUCTURED_HISTORY_FILE, index=False)
        else:
            df_new_history.to_csv(STRUCTURED_HISTORY_FILE, mode="a", header=False, index=False)
            
        state["log_report"] = f"Processed & structured {len(parsed_list)} log lake streams successfully."
    except Exception as e:
        state["log_report"] = f"Agent mapping failed: {e}. Raw response data context: {response.content}"
    return state

def build_consumer_agent():
    workflow = StateGraph(ConsumerState)
    workflow.add_node("fetch_data", fetch_landing_zone_data)
    workflow.add_node("enforce_schema", enforce_schema_and_write)
    workflow.add_edge(START, "fetch_data")
    workflow.add_edge("fetch_data", "enforce_schema")
    workflow.add_edge("enforce_schema", END)
    return workflow.compile()