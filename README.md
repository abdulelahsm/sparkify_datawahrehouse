# Sparkify Data Warehouse 

The aim of this project is to build an ETL pipeline that extracts sparkify's data from S3, stage them in Redshift, and transform them into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


# Project Files

- create_table.py creates the fact and dimension tables in Redshift.

- sql_queries.py defines SQL statements which are used by etl.py

- etl.py laods the data from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.

- README.md contains general information on the project (current file).



## Database Schema

* Staging Tables

* staging_events

* staging_songs


#### Fact Table

- songplays - records in event data associated with song plays 
- NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

users - users in the app - user_id, first_name, last_name, gender, level

songs - songs in music database - song_id, title, artist_id, year, duration

artists - artists in music database - artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday



# How to run the program

- You will need to fill the information, and save it as dwh.cfg then run the program.
 
[CLUSTER]

HOST=''

DB_NAME=''

DB_USER=''

DB_PASSWORD=''

DB_PORT=5439

[IAM_ROLE]

ARN=

[S3]

LOG_DATA='s3://udacity-dend/log_data'

LOG_JSONPATH='s3://udacity-dend/log_json_path.json'

SONG_DATA='s3://udacity-dend/song_data'

[AWS]

KEY=

SECRET=

[DWH]

DWH_CLUSTER_TYPE       = multi-node

DWH_NUM_NODES          = 4

DWH_NODE_TYPE          = dc2.large

DWH_CLUSTER_IDENTIFIER = 

DWH_DB                 = 

DWH_DB_USER            = 

DWH_DB_PASSWORD        = 

DWH_PORT               = 5439

DWH_IAM_ROLE_NAME      = 
