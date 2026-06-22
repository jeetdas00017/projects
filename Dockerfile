FROM apache/airflow:2.10.0

USER root

# Install system dependencies for dbt compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-dev \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt using Airflow provider packages (compatible versions)
RUN pip install --no-cache-dir \
    apache-airflow-providers-dbt-cloud \
    dbt-core \
    dbt-postgres \
    dbt-snowflake
