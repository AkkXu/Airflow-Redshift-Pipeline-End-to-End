# End-to-End Data Pipeline with Airflow & Redshift

This project demonstrates an end-to-end automated data pipeline built with
**Apache Airflow** and **Amazon Redshift**.

The pipeline extracts JSON data from **Amazon S3**, stages it in Redshift,
transforms the data into fact and dimension tables, and validates the results
using automated data quality checks.

---

## Architecture Overview

- **Source**: JSON files stored in Amazon S3  
- **Orchestration**: Apache Airflow  
- **Data Warehouse**: Amazon Redshift (Serverless)  
- **Transformations**: SQL-based ELT  
- **Validation**: Custom Data Quality Operator  

---

## DAG Workflow

1. Stage events data from S3 to Redshift  
2. Stage songs data from S3 to Redshift  
3. Load the `songplays` fact table  
4. Load dimension tables (`users`, `songs`, `artists`, `time`)  
5. Run data quality checks  

The DAG is fully automated and designed to support scheduled execution.

---

## Scheduling

The pipeline is scheduled to run hourly with retry logic enabled to handle transient failures.

---

## Data Quality Checks

- Ensure the `songplays` fact table contains records  
- Ensure the `users` table does not contain NULL user IDs  

If any check fails, the DAG fails and retries automatically.

---

## Technologies Used

- Apache Airflow  
- Amazon Redshift (Serverless)  
- Amazon S3  
- SQL  
- Python  

---

## Screenshots

Screenshots of the Airflow DAG, successful pipeline runs, and Redshift tables
are available in the `screenshots/` directory.

---

## Notes

1.This project was developed as part of the **Udacity Data Engineering Nanodegree**
and extended to follow production-style best practices.

2.This project demonstrates practical experience in building production-style
data pipelines using Airflow, Redshift, and SQL-based transformations.
