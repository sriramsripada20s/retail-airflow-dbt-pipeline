"""
Retail pipeline: ingest -> quality gate -> dbt transform -> quality gate.

Task implementations live in include/ (snowflake_tasks.py, soda_tasks.py) and
include/sql/ — this file only wires them together.
"""

import logging

# Core Airflow DAG and TaskGroup imports
from airflow.sdk import dag, TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

# Astronomer Cosmos imports for running dbt models inside Airflow
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

# Custom helper tasks for Snowflake operations
from include.snowflake_tasks import (
    upload_csv_to_stage,
    check_stage_row_count,
    verify_table_row_count,
    SNOWFLAKE_CONN_ID,
)

# Custom helper tasks for Soda data quality checks
from include.soda_tasks import check_load, check_transform


# Define the Airflow DAG metadata and configuration
@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,   # Disable backfilling
    tags=["retail"],
    template_searchpath=["/usr/local/airflow/include/sql"],  # Folder containing external .sql files
)
def retail():

    # =========================================================================
    # TASK GROUP 1: Ingestion Phase (Upload CSV to Stage -> Load into Snowflake)
    # =========================================================================
    with TaskGroup(group_id="ingest") as ingest:
        
        # Upload local dataset file to Snowflake internal stage
        upload = upload_csv_to_stage()
        
        # Ensure staged CSV has readable records before touching database tables
        stage_check = check_stage_row_count()

        # Create target database table if it doesn't exist
        create_raw_table = SQLExecuteQueryOperator(
            task_id="create_raw_table",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_raw_table.sql",
        )
        
        # Truncate target raw table to guarantee full-refresh idempotency (no duplicates)
        truncate_raw_table = SQLExecuteQueryOperator(
            task_id="truncate_raw_table",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="truncate_raw_table.sql",
        )
        
        # Execute COPY INTO statement to populate RAW_INVOICES table
        copy_into_raw = SQLExecuteQueryOperator(
            task_id="copy_into_raw",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="copy_into_raw.sql",
        )
        
        # Verify row counts and log copy execution history
        verify = verify_table_row_count()

        # Ingestion internal task dependencies
        upload >> stage_check >> create_raw_table >> truncate_raw_table >> copy_into_raw >> verify

    # =========================================================================
    # TASK GROUP 2: Raw Data Quality Gate (Soda Checks on Raw Tables)
    # =========================================================================
    with TaskGroup(group_id="quality_raw") as quality_raw:
        # Run Soda checks on the raw landed data prior to running dbt
        check_load()

    # =========================================================================
    # TASK GROUP 3: Data Transformation (dbt Execution via Astronomer Cosmos)
    # =========================================================================
    # Automatically parses and executes dbt models into DAG tasks
    transform = DbtTaskGroup(
        group_id="transform",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models"],
            dbt_deps=False,
        ),
        operator_args={"install_deps": False},
    )

    # =========================================================================
    # TASK GROUP 4: Transformed Quality Gate (Soda Checks on Marts/Dimensions)
    # =========================================================================
    with TaskGroup(group_id="quality_transform") as quality_transform:
        # Run Soda checks on silver/gold transformation models (dim_*, fct_*)
        check_transform()

    # =========================================================================
    # PIPELINE FLOW DEPENDENCIES
    # =========================================================================
    # Sequence: Ingest -> Raw Quality Gate -> dbt Transformations -> Transform Quality Gate
    ingest >> quality_raw >> transform >> quality_transform


# Register DAG with Airflow runtime
retail()