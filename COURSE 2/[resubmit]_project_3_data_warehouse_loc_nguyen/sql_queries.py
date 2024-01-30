import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
SONG_DATA = config['S3']['SONG_DATA']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_song;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_song_play"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_event(
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER 
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_song(
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_song_play(
    song_play_id    INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL SORTKEY DISTKEY,
    user_id         INTEGER NOT NULL,
    song_id         VARCHAR NOT NULL,
    artist_id       VARCHAR NOT NULL,
    session_id      INTEGER NOT NULL,
    location        VARCHAR,
    user_agent      VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user(
    user_id     INTEGER NOT NULL PRIMARY KEY SORTKEY,
    first_name  VARCHAR NOT NULL,
    last_name   VARCHAR NOT NULL,
    gender      VARCHAR NOT NULL,
    level       VARCHAR NOT NULL
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song(
    song_id     VARCHAR NOT NULL PRIMARY KEY SORTKEY,
    artist_id   INTEGER NOT NULL,
    title       VARCHAR NOT NULL,
    duration    FLOAT,
    year        INTEGER
) DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist(
    artist_id   VARCHAR NOT NULL PRIMARY KEY SORTKEY,
    name        VARCHAR NOT NULL,
    location    VARCHAR,
    latitude    FLOAT,
    longitude   FLOAT
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE dim_time(
    start_time  TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
    hour        INTEGER NOT NULL,
    day         INTEGER NOT NULL,
    week        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    weekday     VARCHAR NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_event FROM {bucket}
CREDENTIALS 'aws_iam_role={role_arn}'
REGION 'us-west-2' 
FORMAT AS JSON {log_json_path}
TIMEFORMAT AS 'epochmillisecs'
""").format(bucket=LOG_DATA, 
            role_arn=ARN, 
            log_json_path=LOG_JSON_PATH)

staging_songs_copy = ("""
COPY stg_song FROM {bucket}
CREDENTIALS 'aws_iam_role={role_arn}'
REGION 'us-west-2' 
FORMAT AS JSON 'auto'                      
""").format(bucket=SONG_DATA, 
            role_arn=ARN)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO fact_song_play (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT e.ts, 
            e.userId, 
            s.song_id, 
            s.artist_id, 
            e.sessionId, 
            e.location, 
            e.userAgent
    FROM stg_event AS e
    JOIN stg_song AS s   
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM stg_event
WHERE user_id IS NOT NULL AND page = 'NextSong'; 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, artist_id, title, duration, year )
SELECT DISTINCT song_id, artist_id, title, duration, year
FROM stg_song
WHERE song_id IS NOT NULL;                    
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM stg_song
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(dayofweek FROM start_time)
FROM fact_song_play;                     
""")


# COUNT ROWS IN EACH TABLE
count_staging_events = ("""SELECT COUNT(*) FROM stg_event""")
count_staging_songs = ("""SELECT COUNT(*) FROM staging_songs""")

count_fact_song_play = ("""SELECT COUNT(*) FROM fact_song_play""")
count_dim_user = ("""SELECT COUNT(*) FROM users""")
count_dim_song = ("""SELECT COUNT(*) FROM songs""")
count_dim_artist = ("""SELECT COUNT(*) FROM artists""")
count_dim_time = ("""SELECT COUNT(*) FROM time""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

count_queries = [count_staging_events, count_staging_songs, count_fact_song_play, count_dim_user, count_dim_song, count_dim_artist, count_dim_time]