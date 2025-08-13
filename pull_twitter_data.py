# -*- coding: utf-8 -*-

#Import necessary libraries
import tweepy, json
import pandas as pd
from pymongo import MongoClient

# Bearer token has been revoked after use
bearer_token = "AAAAAAAAAAAAAAAAAAAAAB2h3QEAAAAAIHQhAeSWcAwGnwkTK1LSMYTWqe8%3DD66h3wx5v92tMPVhaqA9v2kxddHQTJrlgRFyQCUVl89cEzYCJd"

# Initialize the client
client = tweepy.Client(bearer_token=bearer_token)

# Query
query = "technology OR climate -is:retweet lang:en"

# Fetch Tweets with Pagination
all_tweets = []
max_results_per_call = 100
target_count = 80 #The maximum tweets i can pull at in a month is 100. We already pulled 20 while testing 
next_token = None

while len(all_tweets) < target_count:
    remaining = target_count - len(all_tweets)
    response = client.search_recent_tweets(
        query=query,
        max_results=min(remaining, max_results_per_call),
        tweet_fields=["created_at", "text", "author_id"],
        next_token=next_token
    )

    if response.data:
        all_tweets.extend(response.data)
        next_token = response.meta.get("next_token")
        if not next_token:
            break
    else:
        print("No tweets found or token is invalid.")
        break

# Convert to DataFrame
tweet_data = [{
    "id": tweet.id,
    "text": tweet.text,
    "author_id": tweet.author_id,
    "created_at": tweet.created_at.isoformat() if tweet.created_at else None
} for tweet in all_tweets]

df = pd.DataFrame(tweet_data)

# Save to JSON File
df.to_json("technology_climate_tweets.json", orient="records", indent=4)



# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tweet_db"]
collection = db["tweets"]

# Load JSON file
with open("technology_climate_tweets.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# Insert into MongoDB
if isinstance(data, list):
    collection.insert_many(data)  # For list of tweets
else:
    collection.insert_one(data)   # For single tweet

print("Data inserted successfully!")