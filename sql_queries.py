import configparser
import os


# Read the config file
config = configparser.ConfigParser()
config.read('dwh.cfg')


# Queries to DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS staging_songplay_table"
user_table_drop = "DROP TABLE IF EXISTS staging_users_table"
song_table_drop = "DROP TABLE IF EXISTS songs_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP  TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events_table (
                         artist VARCHAR(255),
                         auth VARCHAR(255),
                         firstname VARCHAR(50),
                         gender  VARCHAR(10), 
                         item_in_session  VARCHAR(255), 
                         lastname  VARCHAR(50), 
                         length numeric, 
                        level character varying(255), 
                        location text, 
                        method  VARCHAR(4), 
                        page character varying(255),    
                        registration character varying(255), 
                        session_id integer, 
                        song text , 
                        status integer, 
                        ts bigint, 
                        user_agent text, 
                        user_id integer);  
                        """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs_table (
                                song_id            varchar,
                                num_songs          integer,
                                title              varchar,
                                artist_name        varchar,
                                artist_latitude    float,
                                year               integer,
                                duration           float,
                                artist_id          varchar,
                                artist_longitude   float,
                                artist_location    varchar
);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songs_play  
    (  
        songplay_id INT GENERATED ALWAYS AS IDENTITY distkey,  
        start_time TIMESTAMP NOT NULL sortkey,
        user_id INT NOT NULL,   
        level VARCHAR(50),   
        song_id VARCHAR(25) NOT NULL,   
        artist_id VARCHAR(25) NOT NULL,   
        session_id VARCHAR(25) NOT NULL,   
        location VARCHAR(500),   
        user_agent TEXT,  
        PRIMARY KEY (songplay_id)       
       )     
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (  
    user_id INT NOT NULL sortkey,  
    first_name VARCHAR(50) NOT NULL,   
    last_name VARCHAR(50) NOT NULL,  
    gender VARCHAR(10),   
    level VARCHAR(50),   
    PRIMARY KEY (user_id));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (  
    song_id VARCHAR(50) NOT NULL sortkey,   
    title VARCHAR(500) NULL,   
    artist_id VARCHAR(25) NOT NULL,  
    year INT,   
    duration NUMERIC,   
    PRIMARY KEY(song_id));
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(50) NOT NULL,   
    artist_name VARCHAR(500) NULL sortkey,   
    location VARCHAR(250),   
    latitude DECIMAL(11,8),   
    longitude DECIMAL(11,8),   
    PRIMARY KEY(artist_id));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_info (  
    start_time TIMESTAMP NOT NULL sortkey,  
    hour INT,   
    day INT,   
    week INT,   
    month INT,   
    year INT,   
    weekday VARCHAR(10),   
    PRIMARY KEY(start_time));
""")

# STAGING TABLES


staging_events_copy = ("""copy staging_events_table 
                          from {}
                          CREDENTIALS 'aws_iam_role={}'
                          json {}
                          COMPUPDATE OFF region 'us-west-2';
                          
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs_table
                          from {} 
                          CREDENTIALS 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-west-2'
                        FORMAT AS JSON 'auto' 
                        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songs_play(start_time, user_id, 
                         level,song_id, artist_id, session_id, location, user_agent)
                         SELECT 
                         TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
                         e.user_id,
                         e.level,
                         s.song_id,
                         s.artist_id,
                         e.session_id,
                         e.location,
                         e.user_agent 
                         FROM staging_events_table e, staging_songs_table s
                         WHERE 
                         e.page='NextSong' AND
                         e.artist = s.artist_name AND
                         e.song = s.title AND
                         e.length = s.duration             
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name,last_name,gender,level)
                     SELECT e.user_id,
                     e.firstname,
                     e.lastname,
                     e.gender,
                     e.level
                     FROM staging_events_table e
                     WHERE e.page='NextSong'
                     
""")

song_table_insert = (""" INSERT INTO songs (song_id,title,artist_id,year,duration)
                     SELECT song_id,
                     title,
                     artist_id,
                     year,
                     duration
                     FROM staging_songs_table
                     WHERE song_id IS NOT NULL
                     
""")

artist_table_insert = (""" INSERT INTO artists
                       (artist_id, artist_name,location,latitude,longitude)
                       SELECT artist_id,
                       artist_name, 
                       artist_location,
                       artist_latitude,
                       artist_longitude
                       FROM staging_songs_table
                       WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
                     INSERT INTO time_info
                     (start_time, hour, day, week, month, year, weekDay)
                    SELECT  start_time, 
                            extract(hour from start_time),
                            extract(day from start_time),
                            extract(week from start_time), 
                            extract(month from start_time),
                            extract(year from start_time), 
                            extract(dayofweek from start_time)
                    FROM songs_play
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
