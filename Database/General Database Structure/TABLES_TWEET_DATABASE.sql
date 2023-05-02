CREATE TABLE tweets (
    tweet_id BIGINT PRIMARY KEY,
    tweet TEXT,
    date_time VARCHAR(40),
    language VARCHAR(10)
    );


CREATE TABLE hashtags (
    hashtag_id BIGINT PRIMARY KEY,
    hashtag VARCHAR(300) collate utf8mb4_bin
    );
    
CREATE TABLE tweet_hashtags (
	tweet_id BIGINT,
    hashtag_id BIGINT,
    count BIGINT,
    PRIMARY KEY(tweet_id, hashtag_id),
    FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE,
    FOREIGN KEY(hashtag_id) REFERENCES hashtags(hashtag_id) ON DELETE CASCADE
    );

