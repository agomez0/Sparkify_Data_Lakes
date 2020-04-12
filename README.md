# Sparkify Data Lakes

Sparkify is a music streaming app that has a constantly increasing user and song database. Sparkify's data is stored in S3, in a directory of JSON logs on user activity and songs metadata.

The purpose of this project is to build an ETL pipeline that extracts data from S3, transforms the data with Spark, and loads the data back into S3. We are formatting the data into a star schema, with five tables total.


## Tables:
We have one facts table:
* **songplays_table:** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month

Four dimensional tables:
* **songs_table:** song_id, title, artist_id, year, duration
* **artists_table:** artist_id, name, location, latitude, longitude
* **users_table:** user_id, first_name, last_name, gender, level
* **time_table:** start_time, hour, day, week, month, year, weekday


### Prerequisite:
* Make sure you have created an S3 bucket named `output-data` to load your transformed data.

## How to Run:
1. Duplicate `dl_template.cfg` and rename it `dl.cfg`.
2. Open `dl.cfg` and input your AWS access key and secret key.
3. Open the command terminal and travel to your downloaded repo.
4. Type in `python etl.py` to run the script that extracts the data from the S3 bucket, formats it into a star schema, and loads it back into S3.


