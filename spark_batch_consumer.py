from pyspark.sql.functions import col, split, count
from pyspark.sql.functions import window, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import SparkSession
import random
topic = ""
print("WELCOME TO SPARK BATCH CONSUMER")
print("Enter the topic you want to choose : \n 1. top_tweets \n 2. top_hashtags \n 3. tweets \n 4. hashtags \n 5. random topic\n")
choice = random.randint(1, 4)
print("The chosen topic is : ", choice)
if choice == 1:
    topic = "top_tweets"
elif choice == 2:
    topic = "top_hashtags"
elif choice == 3:
    topic = "tweets"
elif choice == 4:
    topic = "hashtags"

spark = SparkSession.builder.appName("TwitterCount").getOrCreate()

# Define the schema for the Kafka topic
schema = StructType([
    StructField("tweet_id", LongType(), True),
    StructField("tweet", StringType(), True),
    StructField("date_time", StringType(), True),
    StructField("language", StringType(), True)
])

# Read the Kafka topic as a streaming DataFrame
df_tweets = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data in the value column
df_tweets_parsed = df_tweets.select(from_json(col("value").cast("string"), schema).alias("tweet_data"), col("timestamp"))

# Extract the fields from the parsed JSON data
df_tweets_processed = df_tweets_parsed.select("tweet_data.*", "timestamp") \
    .withColumn("date_time", to_timestamp("date_time", "yyyy-MM-dd HH:mm:ss"))

# Define the window duration for the batch processing
window_duration = "5 minutes"

# Group the tweets by language and timestamp window and count the tweets
df_tweet_counts = df_tweets_processed \
    .withWatermark("timestamp", window_duration) \
    .groupBy(window("timestamp", window_duration), "language") \
    .agg(count("*").alias("tweet_count"))

# Print the results to the console
query_tweets = df_tweet_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for the query to terminate
query_tweets.awaitTermination()
