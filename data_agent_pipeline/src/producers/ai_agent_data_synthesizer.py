import json
import os
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from src.producers.redis_keyval_producer import send_redis_event
from src.producers.neo4j_graph_producer import send_neo4j_event
from src.producers.mongo_documentdb_producer import send_mongo_event

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

def run_synthetic_production_agent(data_mode):
    """
    Agent 1 Core: Synthesizes a targeted credit card transaction event 
    matching the requested database format and routes it to the specific Kafka topic.
    """
    # Initialize the local Ollama structural JSON runtime configuration
    llm = ChatOllama(model="llama3", temperature=0.7, format="json", base_url=OLLAMA_BASE_URL)
    
    # Doubled curly braces escape the static JSON fields from LangChain's input validation
    prompt_rules = ""
    if data_mode == "Graph (Neo4j)":
        prompt_rules = """Structure the output for a GRAPH model (Neo4j CDC format). 
        You must return a valid raw JSON object matching this exact structure:
        {{
          "payload": {{
            "start": {{ "id": "AlphanumericHolderName" }},
            "end": {{ "id": "VendorRetailerName" }},
            "properties": {{ "amount": float_value }}
          }}
        }}"""
    elif data_mode == "Document (MongoDB)":
        prompt_rules = """Structure the output for a DOCUMENT model (MongoDB format). 
        You must return a hierarchical valid raw JSON object matching this exact structure:
        {{
          "tx_id": "automated-uuid",
          "user": {{ "id": "AlphanumericHolderName", "country": "US" }},
          "merchant": {{ "name": "VendorRetailerName" }},
          "amount": float_value
        }}"""
    else:
        prompt_rules = """Structure the output for a KEY-VALUE model (Redis layout). 
        You must return a flat valid raw JSON object matching this exact structure:
        {{
          "user_id": "AlphanumericHolderName",
          "merchant_id": "VendorRetailerName",
          "amount": float_value,
          "velocity_flag": "false"
        }}"""

    # Pure concatenation bypasses Python's f-string parsing behaviors completely
    system_instruction = "You are an AI financial data generation engine. " + prompt_rules + " Do not include markdown code block ticks. JSON string only."

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_instruction)
    ])
    
    # 💥 THE TYPO FIX: Execute a clean, direct LangChain invocation pipe chain
    chain = prompt | llm
    response = chain.invoke({})
    
    try:
        clean_text = response.content.strip().lstrip("```json").rstrip("```").strip()
        data = json.loads(clean_text)
        
        # Parse output objects and pass metrics to separate standalone topics
        if data_mode == "Graph (Neo4j)":
            send_neo4j_event(data['payload']['start']['id'], data['payload']['end']['id'], data['payload']['properties']['amount'])
            user, amount = data['payload']['start']['id'], data['payload']['properties']['amount']
        elif data_mode == "Document (MongoDB)":
            send_mongo_event(data['tx_id'], data['user']['id'], data['merchant']['name'], data['amount'])
            user, amount = data['user']['id'], data['amount']
        else:
            send_redis_event(data['user_id'], data['merchant_id'], data['amount'])
            user, amount = data['user_id'], data['amount']
            
        return {"status": "success", "user": user, "amount": amount, "mode": data_mode,  "raw_generated_json": data}
    except Exception as e:
        return {"status": "error", "message": f"Synthesis parsing break: {str(e)}. Raw model reply was: {response.content}"}