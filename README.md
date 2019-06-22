# Udacity-MillionSongDataset-Spark
ETL project for Pyspark

## Introduction

The purpose of the project is to extract from Amazon S3 the song descriptions and the usage logs.
The information is loaded in several Parquet files written to Amazon S3. Namely:
* <code>songs</code> contains information about the songs played
* <code>artitsts</code> contains information about the artists of the played songs
* <code>users</code> contains the information about the users playing the songs
* <code>time</code> is the sparse calendar of the usage of the service
* <code>songplays</code> is the information about the playing of songs

The processing engine is SparkSQL.

## Contents of the project

### dl.cfg

In the AWS section of this configuration file two parameters are defined:
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

This file is not provided as it contains sensitive information.

### etl.py

This file contains the ETL job in Pyspark.

#### create_spark_session
This procedure has two arguments the key id and the access key.
It creates and configures the spark session used throughout the ETL job.


#### process_song_data
This procedure has as arguments the session, the input path and the output path.
It extracts the JSON files describing the songs from Amazon S3 <code>udacity-dend</code> bucket in the <code>song_data</code> directory and loads into the Amazon S3 <code>nicolas-dend</code> bucket the parquet files <code>songs</code> and <code>artists</code>.

#### process_log_data
This procedure has as arguments the session, the input path and the output path.
It extracts the JSON log files from Amazon S3 <code>udacity-dend</code> bucket in the <code>log_data</code> directory and loads into the Amazon S3 <code>nicolas-dend</code> bucket the parquet files <code>users</code>, <code>time</code> and <code>songplays</code>

## Usage
The job is launched with <code>python3 etl.py</code> when in the directory of the <code>etl.py</code> file.
The source and destination is set inside this file.
