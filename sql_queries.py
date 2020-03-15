import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
event_id INT IDENTITY(0,1), 
artist VARCHAR,
auth VARCHAR, 
first_name VARCHAR, 
gender VARCHAR,
item_in_session INT,
last_name VARCHAR, 
length FLOAT,
level VARCHAR , 
location VARCHAR, 
method VARCHAR, 
page VARCHAR,
registration FLOAT,
session_id INT SORTKEY DISTKEY,
song VARCHAR,
status INT,
ts BIGINT,
user_agent VARCHAR,
user_id INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs VARCHAR,
artist_id VARCHAR SORTKEY DISTKEY, 
artist_latitude FLOAT, 
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR, 
song_id VARCHAR,
title VARCHAR , 
duration FLOAT, 
year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INT IDENTITY(0,1) SORTKEY,
start_time TIMESTAMP NOT NULL, 
user_id INT NOT NULL DISTKEY, 
level VARCHAR, 
song_id VARCHAR,
artist_id VARCHAR , 
session_id INT, 
location VARCHAR, 
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id INT SORTKEY,
first_name VARCHAR,
last_name VARCHAR, 
gender VARCHAR,
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR SORTKEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR SORTKEY,
artist_name VARCHAR,
artist_location VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP SORTKEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
)
""")

# STAGING TABLES

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto'
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])


staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON {}
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, 
session_id, location, user_agent)
SELECT 
TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
e.user_id,
e.level,
s.song_id,
e.artist,
e.session_id,
e.location,
e.user_agent
FROM staging_events e
LEFT JOIN staging_songs s
ON e.song = s.title
AND e.artist = s.artist_name
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
WITH unique_user AS (
    SELECT user_id,
    first_name,
    last_name,
    gender,
    level,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) as index
    FROM staging_events
    WHERE page = 'NextSong'
)
SELECT user_id,
first_name,
last_name,
gender,
level
FROM unique_user 
WHERE unique_user.index = 1
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude    
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
WITH time_parse AS
(
    SELECT
    DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events
    WHERE page = 'NextSong'
)
SELECT DISTINCT
start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(weekday FROM start_time) as weekday
FROM time_parse
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
