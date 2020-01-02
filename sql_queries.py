import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events(
        userId              INTEGER 
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER SORTKEY DISTKEY,
        length              FLOAT,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
    );
""")
staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs
(
        artist_id                VARCHAR
        ,artist_latitude          FLOAT
        ,artist_longitude         FLOAT
        ,artist_location          VARCHAR
        ,artist_name              VARCHAR
        ,song_id                  VARCHAR
        ,num_songs                INTEGER
        ,title                    VARCHAR
        ,duration                 FLOAT
        ,year                     INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
        (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY
        ,start_time    TIMESTAMP NOT NULL sortkey
        ,user_id VARCHAR NOT NULL
        ,level VARCHAR
        ,song_id VARCHAR 
        ,artist_id VARCHAR
        ,session_id INTEGER
        ,location VARCHAR
        ,user_agent TEXT
        );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (
    user_id VARCHAR PRIMARY KEY
    ,title VARCHAR
    ,artist_id VARCHAR
    ,year INT
    ,DURATION FLOAT
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (
     song_id VARCHAR PRIMARY KEY 
    ,name VARCHAR
    ,title VARCHAR
    ,artist_id VARCHAR
    ,year INTEGER
    ,duration FLOAT
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (
    artist_id VARCHAR PRIMARY KEY  
    ,name VARCHAR
    ,location VARCHAR
    ,latitude FLOAT
    ,longitude FLOAT 
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time TIMESTAMP PRIMARY KEY SORTKEY 
    ,name VARCHAR(255),
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month TEXT,
    YEAR INTEGER,
    weekday TEXT);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(user_id,title,first_name,last_name,gender,level)
SELECT DISTINC se.userid,se.firstname,se.lastname,se.gender,se.level
FROM staging_events se WHERE se.userid IS NOT NULL AND  se.page='NextSong';
""")
song_table_insert = ("""
    INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id,ss.title,ss.artist_id,ss.year,ss.duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (artist_id, name, location, lattitude, longitude)
    SELECT  DISTINCT ss.artist_id,ss.artist_name,ss.artist_location,ss.artist_latitude,ss.artist_longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second' AS start_time, 
    EXTRACT (hour from sp.start_time),
    EXTRACT (day from sp.start_time) ,
    EXTRACT (week from sp.start_time),
    TO_CHAR (sp.start_time, 'MONTH'),
    EXTRACT (year from sp.start_time),
    TO_CHAR (sp.start_time, 'DAY')
    FROM songplays sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
