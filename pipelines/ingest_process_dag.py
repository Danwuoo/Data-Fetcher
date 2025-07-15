from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(schedule_interval="0 */6 * * *", start_date=datetime(2024, 1, 1), catchup=False)
def ingest_process_dag():
    """示範的 Airflow DAG，包含四個基本任務"""

    @task
    def fetch_raw():
        """抓取原始資料"""
        # TODO: implement actual fetching logic
        pass

    @task
    def convert_parquet():
        """轉換為 Parquet 格式"""
        # TODO: implement conversion logic
        pass

    @task
    def run_pipeline():
        """執行資料處理流程"""
        # TODO: implement processing pipeline
        pass

    @task
    def load_warm():
        """載入到 Warm 層"""
        # TODO: implement warm tier loading
        pass

    fetch_raw() >> convert_parquet() >> run_pipeline() >> load_warm()


dag = ingest_process_dag()
