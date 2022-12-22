"""File that runs the ETL process."""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


def create_config():
    """function that creates the configurations for the ETL process."""
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    INPUT_PATH = config['AWS']['INPUT_PATH']
    OUTPUT_PATH = config['AWS']['OUTPUT_PATH']

    # uncomment for local mode
    #INPUT_PATH = 'data'
    #OUTPUT_PATH = 'output'
    
    print('config done')
    
    return INPUT_PATH, OUTPUT_PATH

def create_spark_session():
    """Create a spark session in AWS."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    print('spark session created')
    
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Load data from song_data dataset and extract songs and artist tables 
    and write the data back into a different bucket in parquet format
    
    Parameters
    ----------
    spark: session
          The spark session that has been created
    input_data: path
           The path to the song_data S3 bucket.
    output_data: path
            The path for the parquet files to be written.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data + '/song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    print('Song dataframe read')
    
    # save the original table in a new view
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
                        SELECT DISTINCT a.song_id
                            ,a.title
                            ,a.artist_id
                            ,a.year
                            ,a.duration
                        FROM songs a
                        """)

    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(path=output_data + '/songs')

    # save the original table in a new view
    df.createOrReplaceTempView("artists")
    
    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT DISTINCT a.artist_id
                                ,a.artist_name as name
                                ,a.artist_location as locaton
                                ,a.artist_latitude as latitude
                                ,a.artist_longitude as longitude
                            FROM artists a                    
                            """)

    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=output_data + '/artists')
    
    print('processed songs')
    

def process_log_data(spark, input_data, output_data):
    """
    Load the data from log_data and extract columns for users and time tables, 
    using the log_data and song_data. The data is written into parquet files 
    amd written on a different S3 as specified in the config file.
    
    Parameters
    ----------
    spark: session
          The spark session that has been created
    input_data: path
           The path to the song_data S3 bucket.
    output_data: path
            The path for the parquet files to be written.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data + '/log_data/*/*/*.json')
    # change path mask for local mode
    #log_data = os.path.join(input_data + '/log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    print('Log dataframe read')
    
    # save the original table in a new view
    df.createOrReplaceTempView("logs")
    
    # filter by actions for song plays and cast userId as integer
    logs_table = spark.sql("""
                        SELECT DISTINCT a.ts
                            ,CAST(a.userId AS int) AS userId
                            ,a.firstName
                            ,a.lastName
                            ,a.gender
                            ,a.level
                            ,a.song
                            ,a.artist
                            ,a.sessionId
                            ,a.location
                            ,a.userAgent
                            ,a.length
                        FROM logs a 
                        WHERE a.page LIKE "NextSong"
                        """)

    logs_table.createOrReplaceTempView('logs')
    
    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT b.userId
                            ,b.firstName
                            ,b.lastName
                            ,b.gender
                            ,b.level
                        FROM (                            
                            SELECT DISTINCT a.userId
                                ,a.firstName
                                ,a.lastName
                                ,a.gender
                                ,a.level
                                ,a.ts
                                ,ROW_NUMBER() OVER (PARTITION BY a.userID ORDER BY a.ts DESC) AS ROWNUM
                            FROM logs a ) b
                        WHERE b.ROWNUM = 1
                        """)

    users_table.createOrReplaceTempView('users')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=output_data + '/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    logs_table = logs_table.withColumn('timestamp', get_timestamp(logs_table.ts))
     
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x) / 1000),TimestampType())
    logs_table = logs_table.withColumn('datetime', get_datetime(logs_table.ts))
    
    # save the original table with new columns once again in a new view
    logs_table.createOrReplaceTempView("logs")
     
    # extract columns to create time table
    time_table = spark.sql("""
                    SELECT DISTINCT a.datetime AS start_time
                        ,hour(a.datetime) AS hour
                        ,day(a.datetime) AS day
                        ,weekofyear(a.datetime) AS week
                        ,month(a.datetime) AS month
                        ,year(a.datetime) AS year
                        ,dayofweek(a.datetime) AS weekday                       
                    FROM logs a
                    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(path=output_data + '/time')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data + '/song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                        SELECT monotonically_increasing_id() as songplay_id
                            ,l.datetime as start_time
                            ,l.userId as user_id
                            ,l.level as level
                            ,s.song_id as song_id
                            ,s.artist_id as artist_id
                            ,l.sessionId as session_id
                            ,l.location as location
                            ,l.userAgent as user_agent
                            ,year(l.datetime) as year
                            ,month(l.datetime) as month
                        FROM logs l
                        LEFT JOIN song_data s
                        ON (l.song = s.title
                            AND l.artist = s.artist_name
                            AND ABS(l.length - s.duration) <2 )
                        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(path=output_data + '/songplays')

    print('processed logs')

def main():
    """
    Main orchestrator function:
    Create a spark session.
    Process song data
    Process log data
    Close spark session
    """
    input_data, output_data = create_config()
    
    spark = create_spark_session()
    
    print('using input folder: {}'.format(input_data))
    print('using output folder: {}'.format(output_data))
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()
    
if __name__ == "__main__":
    main()
