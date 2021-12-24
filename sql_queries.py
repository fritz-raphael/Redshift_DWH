import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

scan_existing_tables = """
SELECT DISTINCT tablename
  FROM PG_TABLE_DEF
 WHERE schemaname = 'public';
"""

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# TRUNCATE TABLES

staging_events_table_truncate = "TRUNCATE TABLE staging_events ;"
staging_songs_table_truncate = "TRUNCATE TABLE staging_songs ;"
songplays_table_truncate = "TRUNCATE TABLE songplays ;"
songs_table_truncate = "TRUNCATE TABLE songs ;"
artists_table_truncate = "TRUNCATE TABLE artists ;"
users_table_truncate = "TRUNCATE TABLE users ;"
time_table_truncate = "TRUNCATE TABLE time ;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
  artist VARCHAR,
  auth VARCHAR,
  firstName VARCHAR,
  gender VARCHAR(5),
  iteminSession INTEGER,
  lastName VARCHAR,
  length DECIMAL,
  level VARCHAR(10),
  location VARCHAR,
  method VARCHAR(10),
  page VARCHAR(20),
  registration VARCHAR,
  sessionId INTEGER,
  song VARCHAR,
  status INTEGER,
  ts BIGINT,
  userAgent VARCHAR,
  userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  num_songs INT,
  artist_id VARCHAR,
  artist_latitude DECIMAL,
  artist_longitude DECIMAL,
  artist_location VARCHAR,
  artist_name VARCHAR,
  song_id VARCHAR,
  title VARCHAR,
  duration DECIMAL,
  year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id INTEGER IDENTITY (1,1) PRIMARY KEY SORTKEY,
  start_time TIMESTAMP NOT NULL,
  user_id INTEGER NOT NULL,
  level VARCHAR,
  song_id VARCHAR NOT NULL DISTKEY,
  artist_id VARCHAR NOT NULL,
  session_id VARCHAR NOT NULL,
  location VARCHAR,
  user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY SORTKEY,
  first_name VARCHAR,
  last_name VARCHAR,
  gender VARCHAR,
  level VARCHAR
  )
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
  song_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
  title VARCHAR,
  artist_id VARCHAR,
  year INTEGER,
  duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY SORTKEY,
  name VARCHAR,
  location VARCHAR,
  latitude DECIMAL,
  longitude DECIMAL
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP PRIMARY KEY SORTKEY,
  hour INTEGER,
  day INTEGER,
  week INTEGER,
  month INTEGER,
  year INTEGER,
  weekday INTEGER
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
SELECT
    timestamp 'epoch' + CAST(s_events.ts/1000 AS BIGINT) * interval '1 second' as start_time,
    s_events.userId,
    s_events.level,
    s_songs.song_id,
    s_songs.artist_id,
    s_events.sessionId,
    s_events.location,
    s_events.userAgent
FROM staging_events AS s_events
     JOIN staging_songs AS s_songs
       ON s_songs.title = s_events.song
      AND s_songs.artist_name = s_events.artist
      AND s_songs.duration = s_events.length
WHERE page = 'NextSong'; """)

user_table_insert = ("""
INSERT INTO users (user_id,
                   first_name,
                   last_name,
                   gender,
                   level)
    SELECT
        DISTINCT(userid) AS user_id,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events AS s1
    WHERE userid IS NOT NULL
      AND ts = (
                SELECT MAX(ts)
                FROM staging_events AS s2
                WHERE s2.userId = s1.userId)
    ;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,
                   title,
                   artist_id,
                   year,
                   duration)
    SELECT
        DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    ;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
                     name,
                     location,
                     latitude,
                     longitude)
    SELECT
        DISTINCT(artist_id) AS artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
        ;
""")

time_table_insert = ("""
INSERT INTO time (start_time,
                  hour,
                  day,
                  week,
                  month,
                  year,
                  weekday)
WITH timetable AS (
    SELECT timestamp 'epoch' + CAST(ts/1000 AS BIGINT) * interval '1 second' as start_time
      FROM staging_events
    )
SELECT
    DISTINCT(start_time) AS start_time,
    DATEPART('hour', start_time) AS hour,
    DATEPART('day', start_time) AS day,
    DATEPART('week', start_time) AS week,
    DATEPART('month', start_time) AS month,
    DATEPART('year', start_time) AS year,
    DATEPART('dayofweek', start_time) AS weekday
FROM timetable;
""")

# CLEAN DATA

set_year_null = ("""
UPDATE songs
   SET year = NULL
 WHERE year = 0
""")

detect_year_zero = ("""
SELECT song_id,
       year
  FROM songs
 WHERE year = 0
 LIMIT 5;
""")

# CHECK FOR DUPLICATES

users_check_duplicates = ("""
WITH duplicates AS (
SELECT COUNT(*) OVER(PARTITION BY user_id) AS num_duplicates,
       user_id,
       first_name,
       last_name,
       gender,
       level
  FROM users
 ORDER BY num_duplicates DESC,
          user_id,
          first_name,
          last_name
 LIMIT 5)
SELECT num_duplicates,
       user_id
  FROM duplicates
 WHERE num_duplicates > 1;
""")

songs_check_duplicates = ("""
WITH duplicates AS (
    SELECT COUNT(*) OVER(PARTITION BY song_id) AS num_duplicates,
           song_id,
           title,
           artist_id,
           year,
           duration
      FROM songs
     ORDER BY num_duplicates DESC,
              song_id,
              title
     LIMIT 5)
SELECT num_duplicates,
       song_id
  FROM duplicates
 WHERE num_duplicates > 1;
""")

artists_check_duplicates = ("""
WITH duplicates AS (
    SELECT COUNT(*) OVER (PARTITION BY artist_id, name) AS num_duplicates,
           artist_id,
           name,
           location,
           latitude,
           longitude
      FROM artists
     ORDER BY num_duplicates DESC,
              name ASC,
              latitude asc,
              longitude asc,
              location desc
     LIMIT 20)
SELECT num_duplicates
  FROM duplicates
 WHERE num_duplicates > 1;
""")

time_check_duplicates = ("""
SELECT COUNT(*) AS num_duplicates,
       start_time
FROM time
GROUP BY start_time
HAVING num_duplicates > 1
ORDER BY COUNT(*) DESC
LIMIT 5;
""")

songplays_check_duplicates = ("""
SELECT COUNT(songplay_id) AS num_duplicates,
       start_time
  FROM songplays
 GROUP BY start_time
HAVING num_duplicates > 1
 ORDER BY num_duplicates DESC
 LIMIT 5;
""")

artists_remove_duplicates = ("""
WITH ordered_duplicates AS (
        SELECT COUNT(*) OVER (PARTITION BY artist_id, name)
                  AS total_duplicates,
               artist_id,
               name,
               location,
               latitude,
               longitude
          FROM artists
         ORDER BY total_duplicates DESC,
                  name ASC,
                  latitude asc,
                  longitude asc,
                  location desc
        ),
     real_duplicates AS (
        SELECT ROW_NUMBER () OVER (PARTITION BY artist_id, name)
                 AS duplicate_row_number,
               *
          FROM ordered_duplicates
         ORDER BY total_duplicates DESC,
                  name,
                  duplicate_row_number,
                  latitude,
                  longitude,
                  location
      )
SELECT * INTO duplicates_table
  FROM real_duplicates
 WHERE total_duplicates > 1
AND duplicate_row_number = 1;

DELETE FROM artists
 USING duplicates_table
 WHERE duplicates_table.artist_id = artists.artist_id
   and duplicates_table.name = artists.name;

INSERT INTO artists (
  artist_id,
  name,
  location,
  latitude,
  longitude
  )
  SELECT
    artist_id,
    name,
    location,
    latitude,
    longitude
  FROM duplicates_table;

DROP TABLE duplicates_table;
""")

# ANALYTIC QUERIES

# Most played artist
songplays_per_artist = ("""
SELECT
    artists.name,
    COUNT(songplays.start_time)
FROM songplays
    JOIN artists
      ON songplays.artist_id = artists.artist_id
GROUP BY artists.name
ORDER BY COUNT(songplays.start_time) DESC
LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
truncate_table_queries = [staging_events_table_truncate,
                          staging_songs_table_truncate,
                          songplays_table_truncate,
                          users_table_truncate,
                          songs_table_truncate,
                          artists_table_truncate,
                          time_table_truncate]
drop_staging_tables_queries = [staging_events_table_drop,
                               staging_songs_table_drop]
copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
check_duplicates_queries= [users_check_duplicates,
                           songs_check_duplicates,
                           artists_check_duplicates,
                           time_check_duplicates,
                           songplays_check_duplicates]
