## Building a Scalable Data Processing Ecosystem on AWS for Alterimac

<ins>1.**Download AWS CLI**:</ins>
- curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
- unzip awscliv21.zip
- sudo ./aws/install
- Configure the CLI
  
<ins>2.**Copy the dataset from S3 to local**:</ins>
- aws s3 cp s3://project-alterimac/music_data.csv /tmp/music_data.csv
  
<ins>3.**Copy data into HDFS**:</ins>
- hdfs dfs -put /tmp/music_data.csv /user
  
<ins>4.**To check if the dataset has been copied**</ins>:
- hdfs dfs -ls /user
  
<ins>5.**Start Hive Terminal**</ins>:
- hive

<ins>6.**Create a table**</ins>:
```markdown

CREATE TABLE song (
        id STRING,
        name STRING,
        Popularity INT,
        Duration_ms INT,
        artists STRING,
        id_artists STRING,
        release_date STRING,
        Danceability FLOAT,
        Energy FLOAT,
        Key STRING,
       Loudness STRING,
        speechiness STRING,
        Acousticness DOUBLE,
         instrumentalness STRING,
        Liveness DOUBLE,
         Valence DOUBLE,
        Time_signature STRING,
         Mood STRING
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY ','
     STORED AS TEXTFILE 
     TBLPROPERTIES ("skip.header.line.count"="1");
```
<ins> 7.**Load data into table**</ins>:
```
LOAD DATA INPATH 's3://project-alterimac/proj 1.csv' INTO TABLE music_tracks;
```

<ins> 8. **Queries on hive**<ins>
- Query to categories based on key and dancebilty:
  ```
  SELECT name, artists, key, danceability,
        CASE 
           WHEN key IN (0, 2, 4, 6, 8, 10) THEN
              CASE 
                 WHEN danceability > 0.7 THEN 'Danceable in Major Key'
                 ELSE 'Less Danceable in Major Key'
              END
           ELSE
              CASE 
                 WHEN danceability > 0.7 THEN 'Danceable in Minor Key'
                 ELSE 'Less Danceable in Minor Key'
              END
        END AS key_danceability_category
  FROM song;
  ```
- To retriew top 5 popular song for each artist
  ```
  SELECT artists, name, Popularity
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY artists ORDER BY Popularity DESC) as rank
    FROM song
  ) ranked_songs
  WHERE rank <= 5
  ORDER BY artists, Popularity DESC;
  ```
- Query to calculate the percentage of songs with acousticness below 0.3 for each time_signature
  ```
  SELECT 
    time_signature,
    MIN(loudness) AS min_loudness,
    MAX(loudness) AS max_loudness,
    AVG(loudness) AS avg_loudness
  FROM 
    song
  GROUP BY 
    time_signature;
  ```
<ins>9.**To get cluster id** </ins>:
- nano /etc/hadoop/conf/core-site.xml

<ins>10. **Jupyter notebook**<ins>:
- Importing necessary libraries and creating spark session
  ```
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import *
  from pyspark.sql.window import Window
  spark = SparkSession.builder \
    .appName("HDFS Data Query") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-10-0-0-217.ec2.internal:8020") \
    .getOrCreate()
  df = spark.read.csv("hdfs://ip-10-0-0-217.ec2.internal:8020/user/music_data.csv", header=True, inferSchema=True)
  ```
- Queries to perform on notebook:
  - Find the 5 top most popular songs for each release year since 2020:
    ```
    df = df.withColumn("release_year", substring("release_date", 1, 4))
    filtered_df = df.filter(col("release_year") >= 2020)
    window_spec = Window.partitionBy("release_year").orderBy(col("popularity").desc())
    ranked_df = filtered_df.withColumn("rank", row_number().over(window_spec))
    top_songs_per_year = ranked_df.filter(col("rank") <= 5)
    top_songs_per_year.select("release_year", "id", "name", "artists", "popularity", "rank").orderBy("release_year","rank").show()
    ```
  - Classify songs into different energy level and further categorize them based on loudness:
    ```
    df = df.withColumn("energy_level",
                   when(col("energy") >= 0.8, "High")
                   .when((col("energy") >= 0.5) & (col("energy") < 0.8), "Medium")
                   .otherwise("Low"))
    df = df.withColumn("loudness_category",
                   when(col("loudness") >= -5.0, "Loud")
                   .when((col("loudness") >= -10.0) & (col("loudness") < -5.0), "Moderate")
                   .otherwise("Soft"))
    df.select("id", "name", "energy", "loudness", "energy_level", "loudness_category").show()
    ```
  - Classify the popularity and calculate the average popularity for each class:
    ```
      df = df.withColumn("popularity_class",
                   when(col("popularity") >= 80, "Very Popular")
                   .when((col("popularity") >= 60) & (col("popularity") < 80), "Popular")
                   .when((col("popularity") >= 40) & (col("popularity") < 60), "Moderately Popular")
                   .otherwise("Less Popular"))
    avg_popularity_df = df.groupBy("popularity_class").agg(avg("popularity").alias("avg_popularity"))
    avg_popularity_df.show()
    ```
  <ins>11.**Queries for hue** <ins>:
  
  - Analyzing loudness value in the dataset:
    ```
    SELECT 
    loudness,
    MIN(loudness) AS min_loudness,
    MAX(loudness) AS max_loudness,
    AVG(loudness) AS avg_loudness
    FROM 
    song
    GROUP BY 
    loudness;
      ```
  - Counting songs by key:
    ```
    SELECT Key, 
    COUNT(*) AS song_count
    FROM song
    GROUP BY Key
    HAVING COUNT(*) > 50;
    ```
  - Query to analyse the average valence (positivity) and liveness for each mood and categorizes the valence
    ```
    SELECT 
    Mood,
    AVG(valence) AS avg_valence,
    AVG(liveness) AS avg_liveness,
    CASE 
        WHEN AVG(valence) > 0.5 THEN 'Above 0.5' 
        ELSE 'Below or Equal to 0.5' 
    END AS valence_above_0_5
    FROM 
    my
    GROUP BY 
    Mood;
    ```
    

