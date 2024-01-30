import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers.sql_queries import SqlQueries

# set args
default_args = {
    "owner": 'Sparkify',
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

# set DAG to hourly
dag = DAG('udac_example_dag',
          default_args=default_args,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path='s3://udacity-dend/log_data',
    region='us-west-2',
    json_option="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_path='s3://udacity-dend/song_data',
    region='us-west-2',
    json_option='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=False,
)

    
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {
            "table": "SELECT COUNT(*) FROM users WHERE userid IS NULL",
            "return": 0,
        },
    ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# set task dependencies
start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator