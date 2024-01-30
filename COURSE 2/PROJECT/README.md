# Datawarehouse with AWS Redshift project
by Loc Nguyen Dang

## Project description

A music streaming app has user activity and song data stored in JSON files on S3. 

This project will build an ETL pipeline to extract the data from S3, stage it in Redshift, and transform it into a set of dimensional tables that can be used to perform analytical tasks.

## Perquisites
Install necessary dependencies: `pip install -r requirements.txt`.
Create IAM role and user on AWS.
Create a cluster in AWS.
Fill the connection information in `dwh.cfg`.

## Steps
1. Define all of the queries in the `sql_queries.py`.
2. Run the `create_tables` script to create the staging table and fact/dim tables of dimensional model.
```Bash
python create_tables.py
```
3. Run the `etl` script to execute the pipeline:
- Extract data from the files in S3 and stage it in Redshift (by running COPY command to implement bulk insert).
- Insert data from staging to the tables in the dimensional tables using `insert into ... select ...`.
```Bash
python etl.py
```

## Project structure

This project includes four script files:

- `sql_queries.py` - SQL statements.
- `create_table.py` - script for the step 1 of the pipeline, staging and fact/dim table are created in Redshift.
- `etl.py` - -step 2 of the pipeline, data loaded from S3 to staging and then inserted into the DWH on Redshift.
- `test.py` - check the number of rows inserted to each table to validate that all the data have been loaded successfully.
- `README.md` - description the overall project.

## Database schema design
The schema will follow the star schema.

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong 
- The fact table `fact_song_play` will have an identity PK `song_play_id` and contains FK which are the PK of dimensional tables. 
- The fact table also contain `location` and `user_agent` for some specific information of a event of the audience.
- `start_time` is selected as SORTKEY because listening event can be grouped or sorted by time-based metrics like date, week, month, etc.
- `start_time` is also selected as DISTKEY because time info is common in every transactions and queries with a high cardinality.
```sql
song_play_id    INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
start_time      TIMESTAMP NOT NULL SORTKEY DISTKEY,
user_id         INTEGER NOT NULL,
song_id         VARCHAR NOT NULL,
artist_id       VARCHAR NOT NULL,
session_id      INTEGER NOT NULL,
location        VARCHAR,
user_agent      VARCHAR
``` 


#### Dimension Tables
- users - users in the app
```sql
user_id     INTEGER NOT NULL PRIMARY KEY SORTKEY,
first_name  VARCHAR NOT NULL,
last_name   VARCHAR NOT NULL,
gender      VARCHAR NOT NULL,
level       VARCHAR NOT NULL
```
- songs - songs in music database
```sql
song_id     VARCHAR NOT NULL PRIMARY KEY SORTKEY,
artist_id   INTEGER NOT NULL,
title       VARCHAR NOT NULL,
duration    FLOAT,
year        INTEGER
```
- artists - artists in music database 
```sql
artist_id   VARCHAR NOT NULL PRIMARY KEY SORTKEY,
name        VARCHAR NOT NULL,
location    VARCHAR,
latitude    FLOAT,
longitude   FLOAT
```
- time - timestamps of records in songplays broken down into specific units 
```sql
start_time  TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
hour        INTEGER NOT NULL,
day         INTEGER NOT NULL,
week        INTEGER NOT NULL,
month       INTEGER NOT NULL,
year        INTEGER NOT NULL,
weekday     VARCHAR NOT NULL
```

## Findings
- I received an error like this when executing `etl.py` scripts:
```Bash
Load into table 'stg_event' failed. Check 'stl_load_errors' system table for details.
```
Then I went to the query editor of Redshift and run `select * from stl_load_errors`, it gave me the message: `Invalid timestamp format or value [YYYY-MM-DD HH24:MI:SS]`.
I was a little bit confused, and after a few google searching, I handled the problem. 
I didn't specify the timestamp format when COPY data from S3 to staging tables. After adding the `TIMEFORMAT AS 'epochmillisecs'` to the copy statement, the script was finally executed.

- The execution time of copying `log_data` to `stg_event` table is very fast, in just about 10 seconds. But copying `song_data`` to `stg_song` is very slow.
I think it could be due to large number of rows of the source data.
It took more than 30 mins for analyzing when set 1 node for Redshift. After increasing to 3 nodes, the processing time was improved but not significant, it still took about 20 mins.