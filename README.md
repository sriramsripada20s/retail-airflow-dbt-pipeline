# Retail Data Pipeline — Snowflake + Airflow 3 + dbt
 
A pipeline that takes a raw retail sales CSV, loads it into Snowflake,
cleans and reshapes it, checks it for quality issues at each stage, builds
reports, and shows them on a dashboard — fully automated via Airflow.
 
## What it does
 
1. Loads the raw CSV into Snowflake
2. Checks the raw data for problems (Soda)
3. Cleans and reshapes it into customer/product/date/sales tables (dbt)
4. Checks the cleaned data for problems (Soda)
5. Builds three summary reports (revenue by country, top products, revenue by month)
6. Checks the reports for problems (Soda)
7. Shows the reports on a dashboard (Metabase)
8. Sends a Slack alert if anything fails, anywhere in the chain
Safe to re-run anytime — it always clears old data before reloading, so
nothing duplicates.
 
## Tools used
 
| Tool | Purpose |
|---|---|
| Apache Airflow 3 | Orchestrates the whole pipeline |
| Snowflake | Database |
| dbt | Cleans and reshapes raw data into a proper model |
| Soda | Data quality checks at three stages |
| Metabase | Dashboard |
| Slack | Failure alerts |

## Pipeline

<img width="876" height="211" alt="image" src="https://github.com/user-attachments/assets/fe0d010d-1121-41d3-b40c-40f0eadc1c10" />


## DAG

<img width="1342" height="623" alt="dag" src="https://github.com/user-attachments/assets/332b71e9-7261-47ae-8c49-ab1036b5ccc0" />

## Phases
 
### Setup
Created a dedicated Snowflake warehouse, database, and schemas, plus a service-account user (`AIRFLOW_RETAIL_USER`) that only Airflow uses — kept separate from any personal login, with just enough permissions to do its job. The Airflow project itself runs locally via Astro CLI, on Airflow 3.

### Ingest
The CSV gets uploaded to a Snowflake internal stage, then copied into a raw table. Before every load, the table is truncated first — so triggering this step any number of times always results in exactly one clean copy of the data, never duplicates.
 
### Quality check 1 (raw layer)
Before trusting the raw data, Soda checks that the expected columns exist, they're the right type, and the row count is close to what's expected (~542K rows). If this fails, the pipeline stops here rather than wasting time transforming bad data.
 
### Transform (dbt)
Raw data passes through three layers: 

**staging** (just renames columns and fixes types, no business logic), 

**intermediate** (applies real rules, like excluding cancelled orders)  

**marts** (the final shape — separate tables for customers, products, and dates, plus one central sales table linking them together).
 
### Quality check 2 (transform layer)
Checks the cleaned tables for internal consistency — no duplicate customer or product keys, and every sale correctly links back to a real customer, product, and date.
 
### Reports
Three summary tables built on top of the cleaned model: revenue by country, best-selling products by quantity, and revenue by month. This is the part meant to actually be read by someone making business decisions.
 
### Quality check 3 (report layer)
Sanity-checks the reports themselves — no negative revenue, no impossible numbers, and at least some data present in each report.
 
### Dashboard
Metabase connects directly to the report tables and turns them into actual charts — a bar chart of revenue by country, a line chart of revenue over time, and a list of top-selling products.

<img width="1907" height="962" alt="dashboard" src="https://github.com/user-attachments/assets/b6298eae-807c-4278-93c0-23e38e5c8027" />

### DAG Error

<img width="1312" height="477" alt="dag_error" src="https://github.com/user-attachments/assets/c21d1169-aedd-490d-9849-f7894f40bcbc" />

### Slack Alerting
If any task in the pipeline fails — a bad load, a broken dbt model, a failed quality check — a message is automatically posted to Slack with the task name and a direct link to the error logs, so failures don't require manually checking the Airflow UI.

<img width="1850" height="628" alt="slack_alert" src="https://github.com/user-attachments/assets/1270e19e-7516-4ee4-aa0c-4299ef03d2a3" />

## Running it
 
1. `astro dev start`
2. Trigger the `retail` DAG at `localhost:8080`
3. View the dashboard at `localhost:3000`
4. Check Slack if anything fails
 
