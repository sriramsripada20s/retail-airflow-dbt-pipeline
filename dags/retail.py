import logging

from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

# Connection ID configured in Airflow to connect to Snowflake
SNOWFLAKE_CONN_ID = "snowflake_retail"

# Initialize logger to record progress and metrics in Airflow task logs
log = logging.getLogger(__name__)


# Define the Airflow DAG metadata and schedule
@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only (no automatic interval)
    catchup=False,   # Do not backfill past runs
    tags=["retail"],
)
def retail():

    # TASK 1: Upload local CSV dataset to Snowflake internal stage
    @task
    def upload_csv_to_stage():
        # Connect to Snowflake using Airflow connection settings
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # Upload local file to stage, compress to GZIP, and overwrite if file exists
        cur.execute(
            "PUT file://include/dataset/Online_Retail.csv @RETAIL.RAW.RETAIL_STAGE "
            "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        
        # Log upload results (source size, target size, and status)
        result = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        for row in result:
            row_dict = dict(zip(columns, row))
            log.info("PUT result: %s", row_dict)
            
        cur.close()

    # TASK 2: Ensure destination raw table exists in Snowflake schema
    create_raw_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS RETAIL.RAW.RAW_INVOICES (
                InvoiceNo   STRING,
                StockCode   STRING,
                Description STRING,
                Quantity    INTEGER,
                InvoiceDate STRING,
                UnitPrice   FLOAT,
                CustomerID  FLOAT,
                Country     STRING
            );
        """,
    )

    # TASK 3: Verify staged CSV file contains valid readable rows before loading
    @task
    def check_stage_row_count():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # Create a reusable file format with matching encoding and carriage returns
        cur.execute("""
            CREATE OR REPLACE FILE FORMAT RETAIL.RAW.STAGE_CHECK_FORMAT
              TYPE = CSV
              SKIP_HEADER = 1
              RECORD_DELIMITER = '\\r'
              ENCODING = 'ISO-8859-1'
        """)
        
        # Count rows in staged file directly without inserting into any table
        cur.execute("""
            SELECT COUNT(*)
            FROM @RETAIL.RAW.RETAIL_STAGE/Online_Retail.csv.gz
            (FILE_FORMAT => 'RETAIL.RAW.STAGE_CHECK_FORMAT')
        """)
        
        row_count = cur.fetchone()[0]
        log.info("Rows visible in staged file: %s", row_count)
        cur.close()
        
        # Stop pipeline immediately if file is empty or corrupt
        if row_count == 0:
            raise ValueError(
                "Staged file contains 0 readable rows. Problem is upstream of COPY INTO "
                "(likely file encoding, line endings, or PUT compression) — check task logs above."
            )
        return row_count

    # TASK 4: Clear target table to prevent duplicate rows on re-runs (Idempotency)
    truncate_raw_table = SQLExecuteQueryOperator(
        task_id="truncate_raw_table",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="TRUNCATE TABLE IF EXISTS RETAIL.RAW.RAW_INVOICES;",
    )

    # TASK 5: Load staged CSV contents into destination table
    copy_into_raw = SQLExecuteQueryOperator(
        task_id="copy_into_raw",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            COPY INTO RETAIL.RAW.RAW_INVOICES
            FROM @RETAIL.RAW.RETAIL_STAGE/Online_Retail.csv.gz
            FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1
                           RECORD_DELIMITER='\\r' ENCODING='ISO-8859-1')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE;  -- Bypass load history since table is truncated first
        """,
    )

    # TASK 6: Audit database row counts and inspect copy execution history
    @task
    def verify_table_row_count():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        # Check total populated rows in RAW_INVOICES
        cur.execute("SELECT COUNT(*) FROM RETAIL.RAW.RAW_INVOICES")
        table_count = cur.fetchone()[0]
        log.info("RAW_INVOICES table row count: %s", table_count)

        # Pull recent Snowflake ingestion history metrics for debugging
        cur.execute("""
            SELECT FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'RETAIL.RAW.RAW_INVOICES',
                START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
            ))
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT 5
        """)
        for row in cur.fetchall():
            log.info("Copy history: %s", row)

        cur.close()
        
        # Raise an error if table is unexpectedly empty
        if table_count == 0:
            raise ValueError(
                "RAW_INVOICES has 0 rows after COPY INTO. See 'Copy history' log lines above "
                "for STATUS/ROW_PARSED/FIRST_ERROR_MESSAGE to pinpoint why."
            )
            
        # Log warning if row count differs from target dataset baseline (541,909)
        expected = 541909
        if table_count != expected:
            log.warning(
                "Row count %s does not match expected %s — check for partial load "
                "or a changed source file.", table_count, expected
            )

    # Instantiate task instances
    upload = upload_csv_to_stage()
    stage_check = check_stage_row_count()
    verify = verify_table_row_count()

    # Define DAG execution dependencies (linear pipeline workflow)
    upload >> stage_check >> create_raw_table >> truncate_raw_table >> copy_into_raw >> verify


# Register DAG with Airflow runtime
retail()