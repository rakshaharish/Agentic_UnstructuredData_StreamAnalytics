# Agentic Unstructured Data Stream Analytics
Enterprise-grade, event-driven multi-source data ingestion pipeline with AI Agent orchestration.

This project leverages an AI Data Synthesizer (Agent-1) to push polymorphic credit card transactions (graph data, document data and key-value data sources) into a multi-topic Kafka broker, processes continuous micro-batches using Apache Spark Structured Streaming, and utilizes a stateful LangGraph AI Consumer (Agent-2) to enforce schema definitions and build a curated, structured historical layer. The entire system is monitored via Streamlit frontend and orchestratable via Apache Airflow. The whole application is containerized using docker-compose.

## Repository Architecture
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
