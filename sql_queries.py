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
CREATE TABLE staging_events (
    event_id           INT     IDENTITY(0,1)     PRIMARY KEY,
    artist             VARCHAR,
    auth               VARCHAR,
    first_name         VARCHAR,
    gender             VARCHAR,
    item_in_session    INT,
    last_name          VARCHAR,
    length             DOUBLE PRECISION,
    level              VARCHAR,
    location           VARCHAR,
    method             VARCHAR,
    page               VARCHAR,
    registration       DOUBLE PRECISION,
    session_id         INT,
    song               VARCHAR,
    status             INT,
    ts                 BIGINT,
    user_agent         VARCHAR,
    user_id            INT)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
artist_id          VARCHAR    PRIMARY KEY,
artist_latitude    FLOAT,
artist_location    VARCHAR,
artist_longitude   FLOAT,
artist_name        VARCHAR,
duration           FLOAT,
num_songs          INT,
song_id            VARCHAR,
title              VARCHAR,
year               INT)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id   BIGINT    IDENTITY    PRIMARY KEY,
start_time    BIGINT    NOT NULL,
user_id       INT       NOT NULL,
level         VARCHAR,
song_id       VARCHAR,
artist_id     VARCHAR,
session_id    VARCHAR,
location      VARCHAR,
user_agent    VARCHAR)
""")

user_table_create = ("""
CREATE TABLE users (
user_id INT   PRIMARY KEY,
first_name    VARCHAR,
last_name     VARCHAR,
gender        VARCHAR,
level         VARCHAR)
""")

song_table_create = ("""
CREATE TABLE songs (
song_id    VARCHAR    PRIMARY KEY,
title      VARCHAR    NOT NULL,
artist_id  VARCHAR,
year       INT,
duration   NUMERIC NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id    VARCHAR    PRIMARY KEY,
name         VARCHAR    NOT NULL,
location     VARCHAR,
latitude     DOUBLE PRECISION,
longitude    DOUBLE PRECISION)
""")

time_table_create = ("""
CREATE TABLE time (
start_time    TIMESTAMP    PRIMARY KEY,
hour          INT,
day           INT,
week          INT,
month         INT,
year          INT,
weekday       INT)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
REGION 'us-west-2'
CREDENTIALS 'aws_iam_role={}' 
json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
REGION 'us-west-2'
CREDENTIALS 'aws_iam_role={}' 
json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT se.ts, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
FROM staging_events se, staging_songs ss 
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT a.start_time, 
EXTRACT (HOUR FROM a.start_time),
EXTRACT (DAY FROM a.start_time),
EXTRACT (WEEK FROM a.start_time),
EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time),
EXTRACT (WEEKDAY FROM a.start_time)
FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
