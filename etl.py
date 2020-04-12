import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

"""
This function initiates a Spark session on the AWS clsuter

"""

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
Function that reads in data from S3 and writes it into
dimensional tables. It then writes the tables to
parquet files.

Parameters:
    spark: spark connection
    input_data: path to S3 location
    output_data: path to S3 output location
"""

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("songs_df")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_df
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT(artist_id), artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_df
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")

"""
Function that reads in data from S3 and writes it into 
dimensional tables, and joins the song and log datasets
to create a fact table. It then writes the tables to 
parquet files.

Parameters:
    spark: spark connection
    input_data: file path to S3 location
    output_data: file path to S3 output location
"""


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    df.createOrReplaceTempView("logs")
  
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT(userId) AS user_id, firstName AS first_name, lastName AS last_name, gender, level
        FROM logs
    """)
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")


    # Filter for song play records on;y
    df = df.filter(df.page == 'NextSong')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/ 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime= udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))

    # View of filtered log data
    df.createOrReplaceTempView("filtered_logs")
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT(datetime) AS start_time, hour(datetime) AS hour, dayofmonth(datetime) AS day, weekofyear(datetime) AS week, month(datetime) AS month, year(datetime) AS year, dayofweek(datetime) AS weekday
        FROM filtered_logs
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # Create a view for songs
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT row_number() over (order by "sessionId") AS songplay_id, l.datetime AS start_time, l.userId AS user_id, l.level, s.song_id, s.artist_id, l.sessionId AS session_id, l.location, l.userAgent AS user_agent, year(datetime) AS year, month(datetime) AS month
        FROM filtered_logs l
        INNER JOIN songs s
        ON l.artist = s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "/songplays/songplays.parquet", mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://output-data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
