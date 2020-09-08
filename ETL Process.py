#!/usr/bin/env python
# coding: utf-8

# In[63]:


from configparser import ConfigParser
import psycopg2
import pandas as pd
import os
import glob
import numpy as np
from sql_queries import *


# In[64]:


# The below function creates an instance of the ConfigParsr class and makes use of its methods to read the config file


# In[65]:


def get_connection_byconfig(filepath,section):
    if (len(filepath)>0 and len(section)>0):
# creates instance of the Config Parser class
        parser=ConfigParser()
# reads the configuration file
        parser.read(filepath)
        diction={}
# if the configuration file has the passed section name
        if (parser.has_section(section)):
# this creates each line in the config file has a separate list. Example ('host','localhost')
            config_params=parser.items(section)
# convert list to python dictionary object
# create empty dictionary
          
            for c in config_params:
                key=c[0]
                value=c[1]
                diction[key]=value
        return diction

     


# In[66]:


d=get_connection_byconfig('Config_ini','postgresql')


# In[67]:


conn = psycopg2.connect(host=d['host'], user=d['user'], dbname="sparkifydb",password=d['password'],port=d['port'])
cur = conn.cursor()


# In[68]:


def get_files(filepath):
    all_files = []
# os.walk method generates filenames in a directory tree
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files


def insert_from_dataframe(df, insert_query):
    for i, row in df.iterrows():
        cur.execute(insert_query, list(row))
    conn.commit()
 


# In[69]:


song_files = get_files('/home/dale/Data Engineer Project/song-data/song_data/')


# In[70]:


len(song_files)


# In[71]:


df=pd.DataFrame()
for song_file in song_files:
    df = df.append(pd.read_json(song_file, lines=True), ignore_index=True)
df.head()


# In[72]:


df.info()


# In[73]:


artist_data=df[['artist_id','artist_name','artist_latitude','artist_longitude','artist_location']]


# In[74]:


artist_data=artist_data.drop_duplicates()


# In[75]:


artist_data.info()


# In[76]:


artist_data = artist_data.replace(np.nan, None, regex=True)


# In[77]:


artist_data.info()


# In[78]:


insert_from_dataframe(artist_data,artists_insert_table)


# In[79]:


song_data = df[['song_id','title', 'artist_id', 'duration', 'year']]
song_data = song_data.drop_duplicates()
song_data = song_data.replace(np.nan, None, regex=True)
song_data.head()


# In[80]:


insert_from_dataframe(song_data,songs_insert_table)


# In[81]:


log_files = get_files('/home/dale/Data Engineer Project/log-data/')


# In[82]:


df=pd.DataFrame()
for log_files in log_files:
    df = df.append(pd.read_json(log_files, lines=True), ignore_index=True)
df


# In[83]:



df = df[df['page'] == 'NextSong']
df = df.replace(np.nan, None, regex=True)
df.head()


# In[84]:



tf = pd.DataFrame({
    'start_time': pd.to_datetime(df['ts'],unit='ms')
})


tf['hour'] = tf['start_time'].dt.hour
tf['day'] = tf['start_time'].dt.day
tf['week'] = tf['start_time'].dt.week
tf['month'] = tf['start_time'].dt.month
tf['year'] = tf['start_time'].dt.year
tf['weekday'] = tf['start_time'].dt.weekday

tf = tf.drop_duplicates()

tf.head()


# In[85]:


tf.info()


# In[86]:


insert_from_dataframe(tf, time_insert_table)


# In[87]:


user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
user_df = user_df.drop_duplicates()
user_df = user_df[user_df['userId'] != '']

user_df.head()


# In[88]:


insert_from_dataframe(user_df, users_insert_table)


# In[89]:



for index, row in df.iterrows():
    print(row.song)
    cur.execute(song_select,(row.song,row.artist,row.length))
    results = cur.fetchone()
    print(results)
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None
    


# In[90]:



for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()
    
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (pd.to_datetime(row.ts,unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
#     songplay_data=df_songplay.values.tolist()[0]
#     songplay_data
#     songplay_data = (df['ts'],df['userId'],df['level'],)
    cur.execute(songplay_insert_table, songplay_data)
    conn.commit()          
               


# In[91]:


conn.close()

