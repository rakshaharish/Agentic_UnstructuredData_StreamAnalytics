import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from src.producers.redis_keyval_producer import send_redis_event
from src.producers.neo4j_graph_producer import send_neo4j_event
from src.producers.mongo_documentdb_producer import send_mongo_event

def run_synthetic_production_agent(openai_api_key):
    """
    Agent 1 Core: Synthesizes high-fidelity financial data and pipes
    the transactional variants to the individual Kafka producers.
    """
    llm = ChatOpenAI(model="gpt-4o", temperature=0.7, openai_api_key=openai_api_key)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are an AI financial data generation engine. 
        Generate one completely randomized, highly realistic credit card transaction.
        Output MUST be valid raw JSON containing exactly three string/numeric properties:
        {{
          "user": "AlphanumericHolderName",
          "merchant": "VendorRetailerName",
          "amount": float_value
        }}
        Do not wrap the output in markdown block ticks (like ```json). JSON string only.""")
    ])
    
    chain = prompt | llm
    response = chain.invoke({})
    
    try:
        clean_text = response.content.strip().lstrip("```json").rstrip("```").strip()
        data = json.loads(clean_text)
        
        # Invoke your individual multi-topic producers
        send_redis_event(data['user'], data['merchant'], data['amount'])
        send_neo4j_event(data['user'], data['merchant'], data['amount'])
        send_mongo_event("tx-automated-id", data['user'], data['merchant'], data['amount'])
        
        return {"status": "success", "user": data['user'], "amount": data['amount']}
    except Exception as e:
        return {"status": "error", "message": str(e)}

