from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="amazon_reviews_big_data_pipeline",
    default_args=default_args,
    schedule_interval=None,   # Manual trigger (important for projects)
    catchup=False,
    tags=["cdac", "spark", "hive", "amazon"]
) as dag:


ingest_raw = BashOperator(
    task_id="ingest_raw",
    bash_command="spark-submit /home/talentum/projects/AmazonReviewAnalytics/BigDataPipeline/spark_jobs/01_ingest_raw.py"
)

write_cleaned = BashOperator(
    task_id="write_cleaned",
    bash_command="/home/talentum/projects/AmazonReviewAnalytics/BigDataPipeline/spark_jobs/02_write_cleaned.py"
)

join_and_curate = BashOperator(
    task_id="join_and_curate",
    bash_command="spark-submit /home/talentum/projects/AmazonReviewAnalytics/BigDataPipeline/spark_jobs/03_join_and_curate.py"
)


write_gold = BashOperator(
    task_id="write_gold",
    bash_command="spark-submit /home/talentum/projects/AmazonReviewAnalytics/BigDataPipeline/spark_jobs/04_write_gold.py"
)



create_hive_table = BashOperator(
    task_id="create_hive_table",
    bash_command="""
    hive -f /home/talentum/projects/AmazonReviewAnalytics/BigDataPipeline/airflow/hive/create_appliance_reviews_curated.hive
    """
)


ingest_raw >> write_cleaned >> join_and_curate >> write_gold >> create_hive_table




















