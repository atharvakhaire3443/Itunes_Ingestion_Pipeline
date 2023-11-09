from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'itunes_ingestion_DAG',
    default_args=default_args,
    description='An Airflow DAG to launch an EMR cluster, add a step, and terminate the cluster',
    schedule_interval=None,
)

def fetch_artist_data(artist,session):
    response = session.get(f"https://itunes.apple.com/search?term={artist}&limit=200")
    if response.status_code == 200:
        artist_data = response.json()
        json_blobs = [json.dumps(item) for item in artist_data['results']]
        return pd.DataFrame({'json_blob': json_blobs})
    else:
        return pd.DataFrame({'json_blob': []})

def pull_from_itunes_and_upload_to_s3(artist_list, bucket_name, s3_key_prefix):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    s3 = boto3.client('s3',aws_access_key_id='your-key',
        aws_secret_access_key='your-secret-key')
    df_list = [fetch_artist_data(artist,session) for artist in artist_list]
    raw_df = pd.concat(df_list, ignore_index=True)
    parquet_buffer = io.BytesIO()
    raw_df.to_parquet(parquet_buffer, index=False)
    s3_key = f"{s3_key_prefix}/raw_itunes.parquet"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())

artist_list = Variable.get('ARTISTS').split(',')
s3_bucket_name = "stock-news-cs777"
s3_key_prefix = "raw_itunes" 


pull_and_store_itunes_data = PythonOperator(
    task_id='pull_and_store_itunes_data',
    python_callable=pull_from_itunes_and_upload_to_s3,
    op_kwargs={
        'artist_list': artist_list,
        'bucket_name': s3_bucket_name,
        's3_key_prefix': s3_key_prefix,
        'aws_access_key': 'your-key',
        'aws_secret_key': 'your-secret-key',
    },
    dag=dag,
)

# Job Flow Overrides
job_flow_overrides = {
    'Name': 'Airflow-EMR',
    'ReleaseLabel': 'emr-5.29.0',
    "LogUri": "s3://dag-emr-logs/",
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker nodes',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=job_flow_overrides,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
)

# Wait for EMR cluster to be active
wait_for_cluster_to_be_ready = EmrJobFlowSensor(
    task_id='wait_for_cluster_to_be_ready',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    target_states=['RUNNING', 'WAITING'],
    failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
    aws_conn_id='aws_default',
    dag=dag,
)

# Add steps to EMR cluster
spark_step = [
    {
        'Name': 'transform_itunes_raw',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://stock-news-cs777/scripts/transform_raw_df.py',
            ],
        },
    },
]

add_steps_to_emr = EmrAddStepsOperator(
    task_id='add_transform_raw_itunes',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=spark_step,
    dag=dag,
)

# Watch step
step_checker = EmrStepSensor(
    task_id='wait_for_transform_raw_itunes',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_transform_raw_itunes', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

spark_step_2 = [
    {
        'Name': 'tracks_split',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://stock-news-cs777/scripts/tracks_split.py',
            ],
        },
    },
]

add_spark_step_2 = EmrAddStepsOperator(
    task_id='add_tracks_split_step',
    job_flow_id="{{task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=spark_step_2,
    dag=dag,
)

spark_step_3 = [
    {
        'Name': 'collections_split',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://stock-news-cs777/scripts/collections_split.py',
            ],
        },
    },
]

add_spark_step_3 = EmrAddStepsOperator(
    task_id='add_collections_split_step',
    job_flow_id="{{task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=spark_step_3,
    dag=dag,
)

spark_step_4 = [
    {
        'Name': 'artists_split',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://stock-news-cs777/scripts/artists_split.py',
            ],
        },
    },
]

add_spark_step_4 = EmrAddStepsOperator(
    task_id='add_artists_split_step',
    job_flow_id="{{task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=spark_step_4,
    dag=dag,
)

step_checker_2 = EmrStepSensor(
    task_id='wait_for_tracks_split',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_tracks_split_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

step_checker_3 = EmrStepSensor(
    task_id='wait_for_collections_split',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_collections_split_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

step_checker_4 = EmrStepSensor(
    task_id='wait_for_artists_split',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_artists_split_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# Terminate EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# Set up dependencies and sequence of tasks
pull_and_store_itunes_data >> create_emr_cluster >> wait_for_cluster_to_be_ready >> add_steps_to_emr >> step_checker

step_checker >> add_spark_step_2
step_checker >> add_spark_step_3
step_checker >> add_spark_step_4

add_spark_step_2 >> step_checker_2
add_spark_step_3 >> step_checker_3
add_spark_step_4 >> step_checker_4

[step_checker_2, step_checker_3, step_checker_4] >> terminate_emr_cluster

# Define the DAG execution
if __name__ == "__main__":
    dag.cli()
