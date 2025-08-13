/*DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
    _id VARCHAR(255),
    author_id BIGINT,
    created_at TIMESTAMP,
    id BIGINT,
    text TEXT,
    sentiment VARCHAR(15) NOT NULL
);*/


SELECT * FROM tweets;

--Count of tweets per sentiment
SELECT sentiment, COUNT(*)
FROM tweets
GROUP BY sentiment;

--Top 10 users by tweet frequency.
SELECT author_id, COUNT(*)
FROM tweets
GROUP BY author_id
ORDER BY COUNT(*) DESC
LIMIT 10;

--Sentiment distribution by day
SELECT 
  DATE(created_at) AS tweet_date,
  sentiment,
  COUNT(*) AS count
FROM tweets
GROUP BY tweet_date, sentiment
ORDER BY tweet_date ASC, sentiment;

--Sentiment distribution by hour
SELECT 
  DATE_TRUNC('hour', created_at) AS tweet_hour,
  sentiment,
  COUNT(*) AS count
FROM tweets
GROUP BY tweet_hour, sentiment
ORDER BY tweet_hour ASC, sentiment;

--Sentiment distribution by minute
SELECT 
  DATE_TRUNC('minute', created_at) AS tweet_hour,
  sentiment,
  COUNT(*) AS count
FROM tweets
GROUP BY tweet_hour, sentiment
ORDER BY tweet_hour ASC, sentiment;