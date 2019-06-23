""" Database SQL statement (Redshift Compatible) 
This script is to define quesries used to create/drop/insert into database.
"""

# Importing System Libraries
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists events_stg;"
staging_songs_table_drop = "drop table if exists songs_stg;"
songplay_table_drop = "drop table if exists songs_plays_fact;"
user_table_drop = "drop table if exists users_dim;"
song_table_drop = "drop table if exists songs_dim;"
artist_table_drop = "drop table if exists artists_dim;"
time_table_drop = "drop table if exists time_dim;"

# CREATE TABLES (Staging)

# Page column as used Diskey because that would help fetch entire dat from one node.
staging_events_table_create= ("""
 create table if not exists events_stg
 (
 artist        varchar(255),
 auth          varchar(255),
 first_name    varchar(255),
 gender        char(1),
 itemInSession smallint,
 last_name     varchar(255),
 length        real,
 level         varchar(20),
 location      varchar(255),
 method        varchar(20),
 page          varchar(20) distkey,
 registration  real,
 session_id    smallint,
 song          varchar(255),
 status        smallint,
 ts            bigint,
 user_Agent    varchar(255),
 user_Id       integer 
 );
 """)

staging_songs_table_create = ("""
create table if not exists songs_stg
(
 num_songs        integer,
 artist_id        varchar(100),
 artist_latitude  real,
 artist_longitude real,
 artist_location  varchar(255),
 artist_name      varchar(255),
 song_id          varchar(100),
 title            varchar(255),
 duration         real,
 year             smallint  
);
""")

# CREATE TABLE (Fact)
# Since data in songs_plays_fact table can more likely be accessed in isolation 
# hence distribution style is even.

songplay_table_create = ("""
create table if not exists songs_plays_fact
(
 songplay_id      integer identity(0,1) primary key,
 start_time       timestamp not null,
 user_id          integer not null,
 level            varchar(100) not null,
 song_id          varchar(255) not null,
 artist_id        varchar(255) not null, 
 session_id       smallint not null,
 location         varchar(255) not null, 
 user_agent       varchar(255) not null
 )
 diststyle even;
 """)
 
# CREATE TABLE (Dimensions)

# Since data in users table is very small hence distribution style is choosen as All.
user_table_create = ("""
create table if not exists users_dim 
(
 user_id          integer primary key,
 first_name       varchar(255) not null,
 last_name        varchar(255),
 gender           char(1) not null,
 level            varchar(100) not null
)
diststyle all;
""")

song_table_create = ("""
create table if not exists songs_dim
(
 song_id          varchar(255) primary key,
 title            varchar(255)  not null,
 artist_id        varchar(255) not null,
 year             integer not null,
 duration         real not null
 );
 """)

artist_table_create = ("""
create table if not exists artists_dim
(
 artist_id        varchar(255) primary key, 
 name             varchar(255) not null, 
 location         varchar(255), 
 latitude         real, 
 longitude        real
);
""")

time_table_create = ("""
create table if not exists time_dim
(start_time      timestamp primary key, 
 hour            smallint not null, 
 day             smallint not null, 
 week            smallint not null, 
 month           smallint not null, 
 year            smallint not null, 
 weekday         smallint not null
 );
 """)

# Insert data into Staging tables

staging_events_copy = ("""
                       copy events_stg from {} 
                       credentials 'aws_iam_role={}' 
                       json 's3://udacity-dend/log_json_path.json' 
                       compupdate off region 'us-west-2'                    
                       """).format(config.get('S3','LOG_DATA'), 
                                   config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
                       copy songs_stg from {} 
                       credentials 'aws_iam_role={}' 
                       json 'auto' 
                       compupdate off region 'us-west-2'                     
                       """).format(config.get('S3','SONG_DATA'),
                                   config.get('IAM_ROLE','ARN'))

# Insert data into Fact table

songplay_table_insert = ("""insert into songs_plays_fact
                         (
                         start_time,
                         user_id,
                         level,
                         song_id,
                         artist_id, 
                         session_id,
                         location, 
                         user_agent
                         )
                         select TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second',
                                user_id,
                                level,
                                s.song_id,
                                a.artist_id,
                                session_id,
                                e.location,
                                user_agent
                         from events_stg e join songs_dim s
                         on (e.song = s.title and e.length = s.duration)
                         join artists_dim a 
                         on (a.name =e.artist)
                         where e.page ='NextSong'; 
                         """)

# Insert data into Dimesnion table

song_table_insert = ("""insert into songs_dim
                     (
                     song_id,
                     title,
                     artist_id,
                     year,
                     duration
                     )
                     select distinct song_id,
                            title,
                            artist_id,
                            year,
                            duration
                       from songs_stg    
                     """)

user_table_insert = ("""insert into users_dim
                       (
                       user_id, 
                       first_name, 
                       last_name, 
                       gender, 
                       level
                       )
                       select distinct user_id,
                              first_name,
                              last_name,
                              gender,
                              level
                         from events_stg
                         where page ='NextSong';    
                       """)
                
artist_table_insert = ("""
                       insert into artists_dim
                       (
                       artist_id, 
                       name, 
                       location, 
                       latitude, 
                       longitude
                       )
                       select distinct artist_id,
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                         from songs_stg;    
                       """)

time_table_insert = ("""
                     insert into time_dim
                     (
                      start_time, 
                      hour, 
                      day, 
                      week, 
                      month, 
                      year, 
                      weekday
                     )
                      select distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second',
                             extract(hour from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
                             extract(day from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
                             extract(week from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
                             extract(month from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
                             extract(year from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
                             extract(weekday from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')
                        from events_stg;   
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,time_table_insert, songplay_table_insert]
