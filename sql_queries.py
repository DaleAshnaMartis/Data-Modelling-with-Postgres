#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Create Tables


# In[2]:


songplay_create_table = (""" 
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY NOT NULL,
start_time timestamp NOT NULL,
user_id int NOT NULL REFERENCES users (user_id),
level varchar,
song_id varchar REFERENCES songs (song_id),
artist_id varchar REFERENCES artists (artist_id),
session_id int NOT NULL,
user_agent varchar,
location varchar
);
""")


# In[3]:


users_create_table=("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")


# In[4]:


songs_create_table=("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
song_name varchar,
artist_id varchar NOT NULL REFERENCES artists (artist_id),
duration float,
year int
);
""")


# In[5]:


artists_create_table=("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY,
artist_name varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar
);

""")


# In[6]:


time_create_table=("""
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")


# In[7]:


# Insert Functions


# In[9]:


songplay_insert_table =("""
INSERT INTO songplays (
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
user_agent,
location
)
VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
""")


# In[10]:


users_insert_table =("""
INSERT INTO users (
user_id,
first_name,
last_name,
gender,
level
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING
""")


# In[11]:


songs_insert_table =("""
INSERT INTO songs (
song_id,
song_name,
artist_id,
duration,
year
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
""")


# In[12]:


artists_insert_table =("""
INSERT INTO artists (
artist_id,
artist_name,
artist_latitude,
artist_longitude,
artist_location
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
""")


# In[13]:


time_insert_table =("""
INSERT INTO time (
start_time,
hour,
day,
week,
month,
year,
weekday
)
VALUES (%s, %s, %s, %s, %s, %s,%s)
ON CONFLICT (start_time) DO NOTHING
""")


# In[14]:


# Drop Tables


# In[15]:


songplay_drop_table = "DROP TABLE IF EXISTS songplays";
users_drop_table = "DROP TABLE IF EXISTS users";
artists_drop_table = "DROP TABLE IF EXISTS artists";
songs_drop_table = "DROP TABLE IF EXISTS songs";
time_drop_table = "DROP TABLE IF EXISTS time";




# In[ ]:

song_select = ("""
    SELECT 
        songs.song_id AS song_id,
        songs.artist_id AS artist_id
    FROM
        songs
        JOIN artists ON (songs.artist_id = artists.artist_id)
    WHERE
        songs.song_name = %s AND 
        artists.artist_name = %s AND 
        songs.duration = %s
""")

create_table_queries = [artists_create_table,songs_create_table,users_create_table,time_create_table,songplay_create_table]
drop_table_queries = [songplay_drop_table,users_drop_table,artists_drop_table,songs_drop_table,time_drop_table]

