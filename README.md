# Agentic Unstructured Data Stream Analytics
Enterprise-grade, event-driven multi-source data ingestion pipeline with AI Agent orchestration.

This project leverages an AI Data Synthesizer (Agent-1) to push polymorphic credit card transactions (graph data, document data and key-value data sources) into a multi-topic Kafka broker, processes continuous micro-batches using Apache Spark Structured Streaming, and utilizes a stateful LangGraph AI Consumer (Agent-2) to enforce schema definitions and build a curated, structured historical layer. The entire system is monitored via Streamlit frontend and orchestratable via Apache Airflow.
