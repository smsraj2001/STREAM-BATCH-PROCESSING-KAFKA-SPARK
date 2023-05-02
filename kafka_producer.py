import random
from kafka import KafkaProducer
import mysql.connector
import time

# connect to the MySQL database
mydb = mysql.connector.connect(
    host="YOUR_HOST_NAME",
    user="YOUR_USER_NAME_OF_DATABASE",
    password="DATABASE_PASSWORD",
    database="DATABASE_NAME"
)

# create a cursor to execute SQL queries
cursor = mydb.cursor()

# initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# continuously publish new tweets and hashtags to Kafka
while True:
    try:
        # select a random number of tweets to retrieve
        num_tweets = random.randint(1, 25)
        
        #----------------------------------------------------------------------------------

        # select all tweets that were added since the last time the loop ran
        sql_tweets = "SELECT tweet_id, tweet, date_time, language FROM tweets WHERE date_time > '2021-01-01 19:51:27 EDT' - INTERVAL 10 SECOND LIMIT 0, 25"
        cursor.execute(sql_tweets)
        # print the SQL query being executed
        print("Executing SQL query for tweets...")
        # publish each new tweet to the Kafka topic
        for tweet_id, tweet, date_time, language in cursor:
            
            message = bytes(f"{tweet_id},{tweet},{date_time},{language}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing tweet:", tweet)
        
            # publish the message to the Kafka topic
            producer.send('tweets', value=message)

        #----------------------------------------------------------------------------------

        # select a random number of hashtags to retrieve
        num_hashtags = random.randint(1, 10)

        # select all new hashtags that were added since the last time the loop ran
        sql_hashtags = "SELECT hashtag_id, hashtag FROM hashtags WHERE hashtag_id < (SELECT MAX(hashtag_id) FROM tweet_hashtags)"
        cursor.execute(sql_hashtags)

        # print the SQL query being executed
        print("Executing SQL query for hashtags...")

        # publish each new hashtag to the Kafka topic
        for hashtag_id, hashtag in cursor:
           # serialize the row data to bytes
            message = bytes(f"{hashtag_id},{hashtag}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing hashtag:", hashtag)
            # publish the message to the Kafka topic
            producer.send('hashtags', value=message)

        #----------------------------------------------------------------------------------

        # select the top 10 tweets with the most hashtags
        sql_top_tweets = "SELECT t.tweet_id, t.tweet, t.date_time, t.language, COUNT(th.hashtag_id) as hashtag_count FROM tweets t JOIN tweet_hashtags th ON t.tweet_id = th.tweet_id GROUP BY t.tweet_id ORDER BY hashtag_count DESC LIMIT 10"
        cursor.execute(sql_top_tweets)

        # print the SQL query being executed
        print("Executing SQL query for top tweets...")

        # publish each top tweet to the Kafka topic
        for tweet_id, tweet, date_time, language, hashtag_count in cursor:
             # serialize the row data to bytes
            message = bytes(f"{tweet_id},{tweet},{date_time},{language},{hashtag_count}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing top tweet:", tweet)
            # publish the message to the Kafka topic
            producer.send('top_tweets', value=message)

        #----------------------------------------------------------------------------------

        # select the hashtags that appear in the top 10 tweets
        sql_top_hashtags = "SELECT h.hashtag_id, h.hashtag, SUM(th.count) as count FROM hashtags h JOIN tweet_hashtags th ON h.hashtag_id = th.hashtag_id JOIN (SELECT t.tweet_id FROM tweets t JOIN tweet_hashtags th ON t.tweet_id = th.tweet_id GROUP BY t.tweet_id ORDER BY COUNT(th.hashtag_id) DESC LIMIT 10) AS subquery ON th.tweet_id = subquery.tweet_id GROUP BY h.hashtag_id ORDER BY count DESC LIMIT 10;"
        
        # execute the SQL query to retrieve the top hashtags
        cursor.execute(sql_top_hashtags)

        # print the SQL query being executed
        print("Executing SQL query for top hashtags...")

        # publish each top hashtag to the Kafka topic
        for hashtag_id, hashtag, count in cursor:
            # serialize the row data to bytes
            message = bytes(f"{hashtag_id},{hashtag},{count}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing top hashtag:", hashtag)
            # publish the message to the Kafka topic
            producer.send('top_hashtags', value=message)
        
        #----------------------------------------------------------------------------------

        # sleep for a random amount of time between 5 and 15 seconds
        time.sleep(random.randint(5, 15))
        
    except Exception as e:
        print(e)
        # in case of any error, sleep for a shorter time and try again
        time.sleep(2)