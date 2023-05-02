from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pandas import *
import random
import time
topic = ""
print("WELCOME TO SPARK STREAMING CONSUMER")
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

# create a SparkSession object
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("id", IntegerType()),
    StructField("text", StringType()),
    StructField("date_time", TimestampType()),
    StructField("language", StringType()),
    StructField("hashtags", StringType())
])

# Create the streaming dataframe
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Define the window and group by clauses
windowedCounts = df \
    .withWatermark("date_time", "15 minutes") \
    .groupBy(
        window(col("date_time"), "15 minutes"),
        col("hashtags")
    ) \
    .agg(count("id").alias("tweet_count"))

# Sort the data in ascending order by the window start time and hashtags
sortedCounts = windowedCounts \
    .sort(
        col("window.start").asc(),
        col("hashtags").asc()
    )

# Write the output to the console
console_query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Write the output to a CSV file
csv_query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.toPandas().to_csv("output.csv", index=False, header=True)) \
    .start()

# Wait for the queries to terminate
console_query.awaitTermination()
csv_query.awaitTermination()

time.sleep(100)

# Stop the queries
console_query.stop()
csv_query.stop()

