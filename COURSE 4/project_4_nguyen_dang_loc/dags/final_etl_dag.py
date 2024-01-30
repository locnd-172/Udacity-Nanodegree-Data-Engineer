from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    "owner": "loc_nguyen",
    "start_date": datetime.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *"
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        create_query=SqlQueries.staging_events_table_create,
        target_table="staging_events",
        s3_path="s3://locnd15-sean-murdock/log-data",
        json="s3://locnd15-sean-murdock/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        create_query=SqlQueries.staging_songs_table_create,
        target_table="staging_songs",
        s3_path="s3://locnd15-sean-murdock/song-data",
        json="auto" 
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        target_table="songplays",
        create_query=SqlQueries.songplays_table_create,
        insert_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        target_table="users",
        create_query=SqlQueries.users_table_create,
        insert_query=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        target_table="songs",
        create_query=SqlQueries.songs_table_create,
        insert_query=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        target_table="artists",
        create_query=SqlQueries.artists_table_create,
        insert_query=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        target_table="time",
        create_query=SqlQueries.times_table_create,
        insert_query=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tests=[
            {"query": "SELECT COUNT(*) FROM songplays WHERE play_id IS NULL", "expected": 0},
            {"query": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected": 0},
            {"query": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", "expected": 0},
            {"query": "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL", "expected": 0}
        ]
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()