"""
Soda data quality check tasks for the retail pipeline.

Each task runs in an isolated virtualenv (soda_venv) via @task.external_python,
since Soda's dependencies are kept separate from the main Airflow environment.
"""

from airflow.sdk import task


# TASK 1: Data quality gate on raw ingested data
# Executes in dedicated Soda virtual environment to avoid package dependency conflicts
@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def check_load(scan_name="check_load", checks_subpath="sources"):
    """Data quality gate on the raw ingested layer (RAW.RAW_INVOICES)."""
    # Deferred import: Executes inside soda_venv environment at runtime
    from include.soda.check_function import check
    
    # Executes Soda scan against source YAML definitions
    return check(scan_name, checks_subpath)


# TASK 2: Data quality gate on dbt dimensional models (Silver/Gold layers)
@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def check_transform(scan_name="check_transform", checks_subpath="transform"):
    """Data quality gate on the dbt-transformed dimensional model (TRANSFORM schema)."""
    from include.soda.check_function import check
    
    # Overrides default data source connection to evaluate TRANSFORM schema models (dim_*, fct_*)
    return check(scan_name, checks_subpath, data_source="retail_transform")


# TASK 3: Data quality gate on final reporting views and aggregate marts
@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def check_report(scan_name="check_report", checks_subpath="report"):
    """Data quality gate on the report layer (TRANSFORM schema, reporting models)."""
    from include.soda.check_function import check
    
    # Evaluates business rules and KPIs on final BI reporting models prior to consumption
    return check(scan_name, checks_subpath, data_source="retail_transform")