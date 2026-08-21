"""
Snowflake ingestion tasks for the retail pipeline.

Kept separate from dags/retail.py so the DAG file stays orchestration-only —
this module holds the actual implementation logic and can be unit tested
independently of Airflow's DAG parsing.
"""

import logging

from airflow.sdk import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Initialize logger to capture execution details in Airflow task logs
log = logging.getLogger(__name__)

# Global configuration constants
SNOWFLAKE_CONN_ID = "snowflake_retail"  # Airflow connection ID for Snowflake
STAGE_FILE = "@RETAIL.RAW.RETAIL_STAGE/Online_Retail.csv.gz"  # Target stage file path
EXPECTED_ROW_COUNT = 541909  # Baseline expected total rows in dataset


@task
def upload_csv_to_stage():
    """Upload the local CSV dataset to the Snowflake internal stage."""
    # Open connection to Snowflake using Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Upload local file to internal stage, auto-compress with GZIP, and overwrite if exists
    cur.execute(
        "PUT file://include/dataset/Online_Retail.csv @RETAIL.RAW.RETAIL_STAGE "
        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )

    # Fetch and log upload metadata (source/target file sizes, upload status)
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    for row in result:
        row_dict = dict(zip(columns, row))
        log.info("PUT result: %s", row_dict)

    cur.close()


@task
def check_stage_row_count():
    """Verify the staged CSV file contains valid readable rows before loading.

    Bypasses COPY INTO / the table entirely — isolates whether a load
    failure originates in the file itself (encoding, line endings) vs.
    the COPY INTO statement.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Create temporary file format matching dataset character encoding and carriage returns
    cur.execute("""
        CREATE OR REPLACE FILE FORMAT RETAIL.RAW.STAGE_CHECK_FORMAT
          TYPE = CSV
          SKIP_HEADER = 1
          RECORD_DELIMITER = '\\r'
          ENCODING = 'ISO-8859-1'
    """)

    # Query row count directly from the staged file
    cur.execute(f"""
        SELECT COUNT(*)
        FROM {STAGE_FILE}
        (FILE_FORMAT => 'RETAIL.RAW.STAGE_CHECK_FORMAT')
    """)

    row_count = cur.fetchone()[0]
    log.info("Rows visible in staged file: %s", row_count)
    cur.close()

    # Fail task if staged file contains zero readable rows
    if row_count == 0:
        raise ValueError(
            "Staged file contains 0 readable rows. Problem is upstream of COPY INTO "
            "(likely file encoding, line endings, or PUT compression) — check task logs above."
        )
    return row_count


@task
def verify_table_row_count():
    """Audit RAW_INVOICES row count and log recent COPY INTO history for debugging."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Fetch total row count from populated target table
    cur.execute("SELECT COUNT(*) FROM RETAIL.RAW.RAW_INVOICES")
    table_count = cur.fetchone()[0]
    log.info("RAW_INVOICES table row count: %s", table_count)

    # Query copy history to inspect load metrics and capture errors if present
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

    # Raise error if no rows loaded into table
    if table_count == 0:
        raise ValueError(
            "RAW_INVOICES has 0 rows after COPY INTO. See 'Copy history' log lines above "
            "for STATUS/ROW_PARSED/FIRST_ERROR_MESSAGE to pinpoint why."
        )

    # Log warning if loaded row count deviates from expected benchmark
    if table_count != EXPECTED_ROW_COUNT:
        log.warning(
            "Row count %s does not match expected %s — check for partial load "
            "or a changed source file.", table_count, EXPECTED_ROW_COUNT
        )