"""
Soda data quality check tasks for the retail pipeline.

Each runs in an isolated virtualenv (soda_venv) via @task.external_python,
since Soda's dependencies are kept separate from the main Airflow environment.
"""
from airflow.sdk import task


@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def check_load(scan_name="check_load", checks_subpath="sources"):
    """Data quality gate on the raw ingested layer (RAW.RAW_INVOICES)."""
    from include.soda.check_function import check
    return check(scan_name, checks_subpath)


@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def check_transform(scan_name="check_transform", checks_subpath="transform"):
    """Data quality gate on the dbt-transformed dimensional model (TRANSFORM schema)."""
    from include.soda.check_function import check
    return check(scan_name, checks_subpath, data_source="retail_transform")
