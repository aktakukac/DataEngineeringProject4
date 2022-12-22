## Data Lake with Spark

### Intro
---
Course: Udacity Nanodegree Data Engineering <br>
Project: Project 4 - Data Lake with Spark <br>
Owner: Mihaly Garamvolgyi <br>
Date: 2022-12-20 <br>

A fictional lake based data warehouse for a startup named Sparkify to analize their song and user data in their database.

### Description
---
A Spark-based application that reads logfiles and song data from Amazon S3 - transforms data and stores the created tables in .parquet files in a different S3 bucket.

### Project Structure
---

```
.
├── dl.cfg       # Configuration file containing AWS IAM credentials and bucket paths
├── etl.py       # Main file that extracts data from S3 and processes using Spark
└── README.md
```

### Dependencies
---
- Python 3
- pyspark


### Schema
---
#### Fact table: <br>
`songplays` - records in log data associated with song plays i.e. records with page NextSong <br>
`songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`


#### Dimension tables: <br>
`users` - users in the app <br>
user_id, first_name, last_name, gender, level
songs - songs in music database<br>
`song_id`, `title`, `artist_id`, `year`, `duration`
artists - artists in music database<br>
`artist_id`, `name`, `location`, `lattitude`, `longitude`
time - timestamps of records in songplays broken down into specific units<br>
`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`




### How to run
---
1. Add credentials and folders to `dl.cfg`
2. Run `etl.py` using Python 3