#
# This is entry point of the Application.
# When DAG is started, raw data is being downloaded, transformed and statistics calculated
# NOTE: All data between Tasks are pased not via XComs but via file system links to file paths!
#
import os
import gdown
import zipfile
import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from misc.utils import format_place_to_parquet, format_outcome_to_parquet
from misc.validator import check_date_month

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
spark_master = "spark://spark-master:7077"

#cwd = os.getcwd()
cwd = '/usr/local'
DATA_DIR = f"{cwd}/datasets"
RAW_FILE_PATH = f"{DATA_DIR}/raw_data.zip"
GOOGLE_DRIVE_URL = "https://drive.google.com/file/d/1ZNoIaUXNDVf9PX--G-ANUEjuj318XtmK/view?usp=share_link"

parquets_path = f"{DATA_DIR}/intermid_parquets"
outcomes_path = f"{parquets_path}/outcome/*"
places_path = f"{parquets_path}/places/*"

merged_data_path = f"{DATA_DIR}/merged_data"
stats_path = f"{DATA_DIR}/statistics"

def downloads_police_dataset_gd(url: str, file_path: str):
    """
    Downloads archive from Google Docs shared folder

    Parameters
    ----------
    url : str
        Google Docs URL for and raw data archive

    file_path : str
        Path to downloaded file on local filesystem
    
    """
    gdown.download(url=url, output=file_path, quiet=False, fuzzy=True)

    file_size = os.path.getsize(file_path)
    # If file size is less 1mb, it must be no data in it
    if file_size < 1000_000: 
        raise ValueError('Downloaded file size is less 1 mb. It might be an empy file!!!')
    return file_path

def prepare_data(data_dir: str, file_path: str):
    """
    Unzips downloaded archive and format each csv file into parquet.
    On this step data sorted out into two folders - Outcome and Places. It is done 
    to be able merge Outcome and Places without sorting

    Parameters
    ----------
    data_dir : str
        Main dir where datasets live

    file_path : str
        Path to downloaded file on local filesystem to unzip
    
    """

    unzip_dir = f"{data_dir}/unzipped"
    intermid_dir = "intermid_parquets"
    
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(unzip_dir)
    
    places_dir = "places"
    outcomes_dir = "outcome"
    intermid_path = "{}/{}/{}/{}"
    
    # format to parquet and reorganize structure
    for (root, dirs, files) in os.walk(unzip_dir, topdown=True):
        if len(dirs):
            continue

        date_month = root.split('/')[-1]
        assert True == check_date_month(date_month)

        # create places dir
        places_path = intermid_path.format(data_dir, intermid_dir, places_dir, date_month)        
        if not os.path.exists(places_path):            
            os.makedirs(places_path, mode=0o777)
        
        # create outcomes dir
        outcomes_path = intermid_path.format(data_dir, intermid_dir, outcomes_dir, date_month)        
        if not os.path.exists(outcomes_path):            
            os.makedirs(outcomes_path, mode=0o777)

        for file in files:
            if 'outcomes' in file:
                format_outcome_to_parquet(src_path=f"{root}/{file}", 
                                  dest_path=f"{outcomes_path}/{file}")
            else:
                format_place_to_parquet(src_path=f"{root}/{file}", 
                                  dest_path=f"{places_path}/{file}")    

    return intermid_dir

now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="police_data_ingestion_dag",   
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['police-data'],
) as dag:

    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=downloads_police_dataset_gd,
        op_kwargs={           
            "url": GOOGLE_DRIVE_URL, 
            "file_path": RAW_FILE_PATH
        },
    )

    unzip_prepare_task = PythonOperator(
        task_id="unzip_prepare_task",
        python_callable=prepare_data,
        op_kwargs={
            "data_dir": DATA_DIR,
            "file_path": RAW_FILE_PATH
        },
    )

    merge_partition_task = SparkSubmitOperator(
        task_id="merge_partition_job",
        application="/usr/local/spark/app/merge_partition.py", 
        name="merge_partition",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[outcomes_path, places_path, merged_data_path],
        dag=dag)

    collect_kpis_task = SparkSubmitOperator(
        task_id="collect_kpis_job",
        application="/usr/local/spark/app/collect_kpis.py",
        name="collect_kpis",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[merged_data_path, stats_path],
        dag=dag)

    download_data_task >> unzip_prepare_task >> merge_partition_task >> collect_kpis_task
