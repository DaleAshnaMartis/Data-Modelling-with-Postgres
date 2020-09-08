# Data-Modelling-with-Postgres

<b>Introduction</b></br>
Sparkify is a music streaming app. The analytics team is particularly interested in understanding what songs users are listening to

The data resides in two main directories:</br>
<b>Songs metadata</b>: collection of JSON files that describes the songs such as title, artist name, year, etc </br>
<b>Logs data</b>: collection of JSON files where each file covers the users activities over a given day
However, this cannot provide an easy way to query the data</br>

<b>The Goal</b></br>
The purpose of this project is to create a Postgres database and ETL pipeline to optimize queries to help Sparkify's analytics team

<b>Database & ETL pipeline</b>
Star Schema
The three most important advantages of using Star schema are:
<ul><li>Denormalized tables</li>
    <li>Simplified queries</li>
    <li>Fast aggregation</li></ul>
   
<b>File Description</b></br>
The source code is available in three separate Python scripts. Below is a brief description of the main files:
<ul>
    <li><b>sql_queries.py</b> has all the queries needed to both create/drop tables for the database as well as a SQL query to get song_id and artist_id from other tables since they are not provided in logs dataset</li>
    <li><b>Create Databse and Tables.ipynb</b> creates the database, establishes the connection and creates/drops all the tables required using sql_queries module</li>
    <li><b>ETL Process.py</b>builds the pipeline that extracts the data from JSON files, does some transformation (such as adding different time attributes from timestamp) and then insert all the data into the corresponding tables</li>
<li>Therefore, we first run Create Databse and Tables.ipynb create the database and tables, and then ETL Process.py to insert the data using the ETL pipeline</li>


