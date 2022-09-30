import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
role = config['IAM_ROLE']['ARN']
logBucket = config['S3']['LOG_DATA']
songBucket = config['S3']['SONG_DATA']
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS times CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events_staging(
    artist VARCHAR(MAX), 
    auth VARCHAR(MAX), 
    firstName VARCHAR(MAX), 
    gender VARCHAR(MAX), 
    itemInSession VARCHAR(MAX), 
    lastName VARCHAR(MAX),
    length DOUBLE PRECISION, 
    level VARCHAR(MAX), 
    location VARCHAR(MAX), 
    method VARCHAR(MAX), 
    page VARCHAR(MAX), 
    registration BIGINT,
    sessionId INTEGER, 
    song VARCHAR(MAX), 
    status INTEGER, 
    ts BIGINT, 
    userAgent VARCHAR(MAX), 
    userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_staging (
    artist_id VARCHAR(MAX),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX),
    song_id VARCHAR(MAX),
    title VARCHAR(MAX),
    duration DOUBLE PRECISION,
    year INTEGER,
    num_songs INTEGER)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INTEGER, 
    location VARCHAR, 
    user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR, 
    year INTEGER, 
    duration DOUBLE PRECISION NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events_staging FROM {}
    credentials 'aws_iam_role={}'
    json 'auto ignorecase' region 'us-west-2'
    """).format(logBucket,role)

staging_songs_copy = ("""
    COPY songs_staging FROM {}
    credentials 'aws_iam_role={}'
    json 'auto ignorecase' region 'us-west-2'
    """).format(songBucket,role)

# FINAL TABLES

songplay_table_insert = ("""
   INSERT INTO songplays(
        start_time, 
        user_id , 
        level , 
        song_id , 
        artist_id , 
        session_id , 
        location , 
        user_agent )
     SELECT
       (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24)) as start_time,
       ev_stg.userId user_id,
       ev_stg.level as level,
       songs_stg.song_id song_id,
       songs_stg.artist_id artist_id,
       ev_stg.sessionId session_id,
       ev_stg.location as location,
       ev_stg.userAgent user_agent
       FROM events_staging ev_stg
       LEFT JOIN songs_staging songs_stg ON (ev_stg.artist=songs_stg.artist_name AND ev_stg.song=songs_stg.title)
       WHERE ev_stg.page='NextSong'
    """)

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT
        distinct
        userId user_id,
        firstName first_name,
        lastName last_name,
        gender,
        level
        FROM events_staging
        where user_id is not null
        order by user_id;
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
        FROM songs_staging
        WHERE song_id is not null;
""")

artist_table_insert = ("""
     INSERT INTO artists(
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude)
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
        FROM songs_staging
        WHERE artist_id is not null;
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    SELECT
        (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))  start_time,
        EXTRACT(hour FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))) as hour,
        EXTRACT(day FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))) as day,
        EXTRACT(week FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))) as week,
        EXTRACT(month FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))) as month,
        EXTRACT(year FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24))) as year,
        EXTRACT(dow FROM (TO_DATE('01/01/1970 00:00:00','DD/MM/YYYY HH24:MI:SS') + (ts/1000/60/60/24)))  as weekday
        FROM events_staging
        WHERE page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
