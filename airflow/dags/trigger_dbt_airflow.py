from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
}

with DAG(
    dag_id="dbt_trigger_dag",
    description="Run DBT models and tests",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake"],
) as dag:

    debug_mounts = BashOperator(
        task_id="debug_mounts",
        bash_command="""
        echo "===== AIRFLOW ====="
        ls -R /opt/airflow 2>/dev/null | head -200 || true

        echo "===== DBT PROJECT ====="
        find / -name dbt_project.yml 2>/dev/null || true
        """,
    )

    check_dbt = BashOperator(
        task_id="check_dbt",
        bash_command="""
        echo "===== DBT VERSION ====="
        which dbt || true
        dbt --version || true

        echo "===== INSTALLED DBT PACKAGES ====="
        pip list | grep dbt || true

        echo "===== PROJECT DIRECTORY ====="
        ls -la /opt/airflow/dags/dbt || true
        """,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt deps --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_staging = BashOperator(
        task_id="run_staging",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_intermediate = BashOperator(
        task_id="run_intermediate",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select int --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_dimensions = BashOperator(
        task_id="run_dimensions",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.dimension --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_facts = BashOperator(
        task_id="run_facts",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.fact --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    (
        debug_mounts
        >> check_dbt
        >> dbt_deps
        >> run_staging
        >> run_intermediate
        >> run_dimensions
        >> run_facts
        >> dbt_test
    )