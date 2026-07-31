import sqlite3
import pandas as pd
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from src.schema_enforcer_agent.graph_state import ConsumerState

def fetch_landing_zone_data(state: ConsumerState) -> ConsumerState:
    try:
        conn = sqlite3.connect('historical_warehouse.db')
        df = pd.read_sql_query("""
            SELECT rowid, source_topic, raw_payload FROM raw_landing_zone 
            WHERE rowid NOT IN (SELECT distinct raw_ref_id FROM structured_history WHERE raw_ref_id IS NOT NULL) 
            LIMIT 10;
        """, conn)
        conn.close()
        state["raw_records"] = df.to_dict(orient="records")
    except:
        state["raw_records"] = []
    return state

def enforce_schema_and_write(state: ConsumerState) -> ConsumerState:
    if not state["raw_records"]:
        state["log_report"] = "No new raw records found to process."
        return state

    llm = ChatOpenAI(model="gpt-4o", temperature=0.1)
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Parse mixed credit card transactional fields into a clean flat schema: user_id, merchant_id, amount.
        Return raw JSON arrays ONLY: [{{"raw_ref_id": rowid, "user_id": "str", "merchant_id": "str", "amount": float}}]
        Do not include markdown codes or parameters outside the array string."""),
        ("user", "Extract parameters from: {records}")
    ])
    
    response = llm.invoke(prompt.format(records=json.dumps(state["raw_records"])))
    
    try:
        clean_out = response.content.strip().lstrip("```json").rstrip("```").strip()
        parsed_list = json.loads(clean_out)
        
        conn = sqlite3.connect('historical_warehouse.db')
        conn.execute("CREATE TABLE IF NOT EXISTS structured_history (raw_ref_id INTEGER, user_id TEXT, merchant_id TEXT, amount REAL)")
        
        for item in parsed_list:
            conn.execute("INSERT INTO structured_history VALUES (?, ?, ?, ?)", 
                         (item.get('raw_ref_id'), item.get('user_id'), item.get('merchant_id'), item.get('amount')))
        conn.commit()
        conn.close()
        state["log_report"] = f"Processed & structured {len(parsed_list)} log streams successfully."
    except Exception as e:
        state["log_report"] = f"Agent mapping failed: {e}"
    return state

def build_consumer_agent():
    workflow = StateGraph(ConsumerState)
    workflow.add_node("fetch_data", fetch_landing_zone_data)
    workflow.add_node("enforce_schema", enforce_schema_and_write)
    workflow.add_edge(START, "fetch_data")
    workflow.add_edge("fetch_data", "enforce_schema")
    workflow.add_edge("enforce_schema", END)
    return workflow.compile()

