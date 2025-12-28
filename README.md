# Automated Data Pipeline with Airflow and Redshift

This project implements an automated ETL pipeline using Apache Airflow and Amazon Redshift.

## Architecture
- Data is staged from S3 into Redshift staging tables
- Fact and dimension tables are built using SQL transformations
- Data quality checks validate pipeline results
- The DAG runs hourly using a cron schedule

## DAG Overview
- Stage_events
- Stage_songs
- Load_songplays_fact_table
- Load dimension tables (users, songs, artists, time)
- Run_data_quality_checks

## Scheduling
The DAG is scheduled to run every hour:
