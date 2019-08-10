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

staging_events_table_create = (
				"""
				CREATE TABLE staging_events(
                    event_id INT IDENTITY(0,1) NOT NULL,
				    artist_name VARCHAR(255),
				    auth VARCHAR(50),
				    user_first_name VARCHAR(255),
				    user_gender  VARCHAR(1),
				    item_in_session	INTEGER,
				    user_last_name VARCHAR(255),
				    song_length	FLOAT(32), 
				    user_level VARCHAR(50),
				    location VARCHAR(255),	
				    method VARCHAR(25),
				    page VARCHAR(35),	
				    registration VARCHAR(50),	
				    session_id	BIGINT,
				    song_title VARCHAR(255),
				    status INTEGER,
				    ts VARCHAR(50),
				    user_agent TEXT,	
				    user_id VARCHAR(100),
				    PRIMARY KEY (event_id))
				"""
				)

staging_songs_table_create = (
					"""
					CREATE TABLE staging_songs(
					    song_id VARCHAR(100),
					    num_songs INTEGER,
					    artist_id VARCHAR(100),
					    artist_latitude FLOAT(32),
					    artist_longitude FLOAT(32),
					    artist_location VARCHAR(255),
					    artist_name VARCHAR(255),
					    title VARCHAR(255),
					    duration DECIMAL,
					    year INTEGER,
					    PRIMARY KEY NOT NULL(song_id))
				"""
				)

songplay_table_create = (
				"""
				CREATE TABLE songplays (
					songplay_id INT IDENTITY(0,1) NOT NULL,
					start_time  TIMESTAMP REFERENCES time(start_time),
					user_id INTEGER REFERENCES users(user_id),
					level VARCHAR(255),
					song_id VARCHAR(255) REFERENCES songs(song_id),
					artist_id VARCHAR(255)REFERENCES artists(artist_id),
					session_id INTEGER,
					location VARCHAR(255),
					user_agent VARCHAR(255))
				"""
				)

user_table_create = (
				"""
				CREATE TABLE users (
					user_id INTEGER PRIMARY KEY NOT NULL,
					first_name VARCHAR(255),
					last_name VARCHAR(255),
					gender VARCHAR(100),
					level VARCHAR(255))
				""")

song_table_create = (
				"""
				CREATE TABLE songs (
					song_id VARCHAR(255) PRIMARY KEY NOT NULL,
					title VARCHAR(255),
					artist_id VARCHAR(255),
					year INTEGER,
					duration DECIMAL)
				""")

artist_table_create = (
				"""
				CREATE TABLE artists (
					artist_id VARCHAR(255) PRIMARY KEY NOT NULL,
					name VARCHAR(255),
					location VARCHAR(255),
					lattitude FLOAT(32),
					longitude FLOAT(32))
				""")

time_table_create = (
				"""
				CREATE TABLE time (
					start_time TIMESTAMP PRIMARY KEY NOT NULL,
					hour INT,
					day INT,
					week INT,
					month INT,
					year INT,
					weekday INT)
				""")

# STAGING TABLES

staging_events_copy = (
				"""
				copy staging_events from '{}'
 				credentials 'aws_iam_role={}'
 				region 'us-west-2' 
 				COMPUPDATE OFF STATUPDATE OFF
 				JSON '{}
 				'""").format(config.get('S3','LOG_DATA'),
							config.get('IAM_ROLE', 'ARN'),
							config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (
				"""
				copy staging_songs from '{}'
			 	credentials 'aws_iam_role={}'
				region 'us-west-2' 
			 	COMPUPDATE OFF STATUPDATE OFF
				JSON 'auto'
				""").format(config.get('S3','SONG_DATA'), 
						config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = (
				"""
				INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
					SELECT
						TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
						staging_events.user_id,
						staging_events.user_level,
						staging_songs.song_id,
						staging_songs.artist_id,
						staging_events.session_id,
						staging_events.location,
						staging_events.user_agent
					FROM staging_events, staging_songs
					WHERE staging_events.page = 'NextSong'
					AND staging_events.song_title = staging_songs.title
					AND user_id NOT IN (SELECT songplays.user_id FROM songplays WHERE songplays.user_id = user_id
										AND songplays.start_time = start_time AND songplays.session_id = session_id )
				""")
 
user_table_insert = (
				"""
				INSERT INTO users (user_id, first_name, last_name, gender, level)  
					SELECT 
						user_id,
						user_first_name,
						user_last_name,
						user_gender, 
						user_level
					FROM staging_events
					WHERE page = 'NextSong'
					AND user_id NOT IN (SELECT user_id FROM users)
				""")

song_table_insert = (
				"""
				INSERT INTO songs (song_id, title, artist_id, year, duration) 
					SELECT 
						song_id, 
						title,
						artist_id,
						year,
						duration
					FROM staging_songs
					WHERE song_id NOT IN (SELECT song_id FROM songs)
				""")

artist_table_insert = (
				"""
				INSERT INTO artists (artist_id, name, location, latitude, longitude) 
					SELECT 
						artist_id,
						artist_name,
						artist_location,
						artist_latitude,
						artist_longitude
					FROM staging_songs
					WHERE artist_id NOT IN (SELECT artist_id FROM artists)
				""")

time_table_insert = (
				"""
				INSERT INTO time (start_time, hour, day, week, month, year, weekday)
					SELECT 
						start_time, 
						EXTRACT(hr from start_time) AS hour,
						EXTRACT(d from start_time) AS day,
						EXTRACT(w from start_time) AS week,
						EXTRACT(mon from start_time) AS month,
						EXTRACT(yr from start_time) AS year, 
						EXTRACT(weekday from start_time) AS weekday 
					FROM (
					SELECT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
					FROM staging_events )
					WHERE start_time NOT IN (SELECT start_time FROM time)
				""")

# QUERY LISTS
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create, staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]