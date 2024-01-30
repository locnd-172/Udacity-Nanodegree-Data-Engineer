# Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

**AWS Connection**
Connection Id: Enter aws_credentials.
Connection Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

**Redshift Connection**
Connection Id: Enter redshift.
Connection Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. 
Schema: This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password created when launching the Redshift cluster.
Port: Enter 5439.

## Data Source 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Project Structure
* dags/etl_dag.py: Defines main DAG, tasks and link the tasks in required order.
* dags/create_tables.sql: SQL create table statements provided with template.
* plugins/helpers/sql_queries.py: Contains SQL queries for the ETL pipeline
* plugins/operators/stage_redshift.py: Operator that copies JSON data from S3 to staging tables in the Redshift
* plugins/operators/load_dimension.py: Operator that loads data from redshift staging tables into dimensional tables
* plugins/operators/load_fact.py: Operator that loads data from redshift staging tables into fact table
* plugins/operators/data_quality.py: Operator that runs data quality checks on all tables passed as parameter
