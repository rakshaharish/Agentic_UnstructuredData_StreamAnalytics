#  🌟 Agentic Unstructured Data Stream Analytics
Enterprise-grade, event-driven multi-source data ingestion pipeline with AI Agent orchestration.

This project leverages an AI Data Synthesizer (Agent-1) to push polymorphic credit card transactions (graph data, document data and key-value data sources) into a multi-topic Kafka broker, processes continuous micro-batches using Apache Spark Structured Streaming, and utilizes a stateful LangGraph AI Consumer (Agent-2) to enforce schema definitions and build a curated, structured historical layer. The entire system is monitored via Streamlit frontend and orchestrated via Apache Airflow, and is containerized using docker-compose.

##  ✨ Concepts & Technologies
1. Python - Development Programming Language
2. Graph Data (neo4j), Key-Value Data (redis), Document Data (mongo) - Types of unstructured data ingested in the project
3. LangChain, LangGraph - AI agent development
4. Kafka, Spark, Pandas, SparkSQL - Streaming and data processing/transformation
5. Streamlit - Frontend Dashboard (UI)
6. Apache Airflow - Orchestration
7. Docker - Containerization & Deployment
8. VS Code - IDE for development

## 📁 Repository Structure
```text
data-agent-pipeline/
├── docker-compose.yml          # Infrastructure: Multi-topic Kafka cluster broker
├── requirements.txt            # Python deployment dependencies
├── historical_warehouse.db     # Embedded Data Lake (Auto-generated landing & history tables)
├── dags/
│   └── pipeline_orchestrator.py # Apache Airflow workflow scheduling script
└── src/
    ├── producers/
    │   ├── ai_agent_data_synthesizer.py      # Agent 1: LLM Multi-Model transactional traffic factory
    │   ├── redis_keyval_producer.py   # Formatter & router for flat key-value strings
    │   ├── neo4j_graph_producer.py   # Formatter & router for relational node/edge structures
    │   └── mongo_documentdb_producer.py   # Formatter & router for nested rich document profiles
    ├── streaming/
    │   └── spark_transformer.py # Spark continuous multi-topic streaming engine
    ├── schema_enforcer_agent/
    │   ├── graph_state.py      # Stateful TypedDict model for LangGraph
    │   └── ai_agent.py         # Agent 2: Stateful LangGraph schema enforcement engine
    └── app/
        └── Streamlit_UI.py               # Streamlit orchestration & monitoring panel
```

🚀 Execution Steps

1. Environment Preparation: Before running the project, run the below commands in the project folder to create and activate python virtual environment.
```text
bash
cd path/to/your/github-data-agent-pipeline

python3 -m venv .venv

.venv\Scripts\Activate.ps1    
```
2. Install all Python requirements locally:
``` text
bash 
pip install -r requirements.txt
```

3. Expose Dashboard Interface: Run the main Streamlit frontend panel:
```text
bash
streamlit run src/app/UI.py
```

4. Boot Environment Containers: Provide an OpenAI API Key in the panel sidebar, then click Boot Infrastructure Setup to start the local multi-topic Kafka cluster broker.

5. Initialize Streaming Transformers: Click Execute Spark Stream Pipeline to launch the PySpark consumer loop listening to your background topics.

6. Synthesize & Push Transactions: Click Synthesize & Route Event. This triggers Agent 1 (synth_agent.py) to build a high-fidelity transaction payload and distribute it to your multi-model producers.

7. Enforce Schema Rules via AI: Click Run Consumer AI Agent Loop to manually prompt Agent 2 (ai_agent.py) to parse, audit, and clean the landing records into your structured analytical reporting table layer.

8. Production Workflow Scheduling: Place the dags/ module folder inside your active Airflow repository directory path to run the data collection and auditing steps automatically every hour.