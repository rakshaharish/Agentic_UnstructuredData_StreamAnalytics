
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

from src.schema_enforcer_agent.ai_agent import build_consumer_agent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def trigger_consumer_agent():
    agent = build_consumer_agent()
    execution_result = agent.invoke({"raw_records": [], "log_report": ""})
    print(f"Airflow Scheduled Execution Log: {execution_result['log_report']}")

default_args = {
    'owner': 'data_platform',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'agent_schema_enforcement_dag',
    default_args=default_args,
    description='Hourly schedule routing LangGraph Schema Cleaners over landing tables',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    run_agent_task = PythonOperator(
        task_id='execute_langgraph_consumer_loop',
        python_callable=trigger_consumer_agent,
    )
