import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, FloatType, TimestampType
import pandas as pd
import datetime
import time


def create_spark_session(k, s):
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4,net.java.dev.jets3t:jets3t:0.9.4") \
        .getOrCreate() 

#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \




    # Thanks to the slack community
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", k)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", s)
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    # read song data file
    print("reading song data")
    df = spark.read.json(song_data)
    print("read song data")

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    song_destination = output_data + 'songs'
    print("writing song data")
    songs_table.write.mode("overwrite").partitionBy('year','artist_id').parquet(song_destination)
    print("song data written")

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id, artist_name name, artist_location location, artist_latitude lattitude, artist_longitude longitude FROM songs
    """)
    
    # write artists table to parquet files
    artists_destination = output_data + 'artists'
    print("writing artists data: " + artists_destination)
    artists_table.write.mode("overwrite").parquet(artists_destination)
    print("artists data written")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    print(log_data)
    # read log data file
    print("reading log data")
    df = spark.read.json(log_data)
    print("log data read")
    fmt = "yyyy-MM-ddThh:mm:ss.SSS"
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, FloatType())
    # create timestamp column from original timestamp column
    df = df.withColumn('start_timestamp', get_timestamp(col('ts')))

    get_datetime = udf(lambda x: datetime.datetime.utcfromtimestamp(x).isoformat())
    df = df.withColumn('start_datetime', get_datetime(col('start_timestamp')))
    
    # filter by actions for song plays
    df.createOrReplaceTempView("logs")
    # logs.start_datetime, logs.start_timestamp, logs.sessionId, logs.method, logs.auth, logs.registration, logs.ts, logs.page, logs.length, logs.level, logs.userAgent, logs.gender, logs.song, logs.location, logs.userId, logs.lastName, logs.itemInSession, logs.artist, logs.firstName, logs.status
    df = spark.sql("""
    SELECT * FROM logs WHERE page = 'NextSong'
    """)

    # extract columns for users table 
    # If some songplays entries were free and other ones paid, keep the paid level
    users_table = spark.sql("""
    SELECT userId user_id, firstName first_name, lastName last_name, gender, MAX(level) level FROM logs
     GROUP BY userId, firstName, lastName, gender
    """)
    
    # write users table to parquet files
    print("writing users data")
    users_table.write.mode("overwrite").parquet(output_data + '/users')
    print("users data written")

    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT ts, start_datetime, start_timestamp, year(start_datetime) year, month(start_datetime) month, dayofmonth(start_datetime) dayofmonth, hour(start_datetime) hour, weekofyear(start_datetime) weekofyear FROM logs
    """)
    
    # write time table to parquet files partitioned by year and month
    print("writing time data")
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time')
    print("time data written")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')
    song_df.createOrReplaceTempView("songs_table")


    # read in artist data to use for songplays table
    artist_df = spark.read.parquet(output_data + 'artists')
    artist_df.createOrReplaceTempView("artists_table")

    # read in time data to use for songplays table
    cal_df = spark.read.parquet(output_data + 'time')
    cal_df.createOrReplaceTempView("calendar")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT t.year, t.month, monotonically_increasing_id() songplay_id, f.start_datetime, f.userId user_id, f.level, 
            s.song_id, a.artist_id, f.sessionId session_id, f.location, f.userAgent user_agent 
      FROM logs f 
      LEFT OUTER JOIN artists_table a ON f.artist = a.name
      LEFT OUTER JOIN songs_table s ON f.song = s.title AND s.artist_id = a.artist_id
      INNER JOIN calendar t ON t.ts = f.ts
    """)

    # write songplays table to parquet files partitioned by year and month
    print("writing log data")
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'songplays')
    print("log data written")

import boto3


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


    print("access key " + config['AWS']['AWS_ACCESS_KEY_ID'])
    print("secret " + config['AWS']['AWS_SECRET_ACCESS_KEY'])

    spark = create_spark_session(config['AWS']['AWS_ACCESS_KEY_ID'], config['AWS']['AWS_SECRET_ACCESS_KEY'])
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nicolas-dend-spark/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

