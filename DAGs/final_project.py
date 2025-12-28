from datetime import timedelta
import pendulum

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, tz="UTC"),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
)
def final_project():

    s3_bucket = Variable.get("s3_bucket")

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key="log-data",
        json_path=f"s3://{s3_bucket}/log_json_path.json",
        region="us-west-2",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key="song-data",
        json_path="auto",
        region="us-west-2",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate_table=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        truncate_table=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate_table=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate_table=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tests=[
            # return 1 when data more than 1 row
            {
                "sql": "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM songplays",
                "expected_result": 1,
            },
            # check userid not null
            {
                "sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL",
                "expected_result": 0,
            },
        ],
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
