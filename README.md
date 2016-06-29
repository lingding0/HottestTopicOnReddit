# Sweet Reddit
======================================

## Personalized Real-time Reddit topic suggestion
[www.leo-ding.com](http://www.leo-ding.com)

Sweet Reddit is a tool to suggest personalized real time Reddit topics to users. The suggestion is based on users' commenting history using the following tecknologies:
- Amazon S3
- Apache Kafka 0.8.2.1
- Spark
- Spark Streaming
- Apache Cassandra
- Redis
- Flask with D3, Bootstrap and Ajax

# What Sweet Reddit Provides:
Sweet Reddit permits users to find real-time Reddit topics that may best fit a user's personal preference. Besed on users historical comments, a list of suggestion will be provided to a user. Also, a user relationship graph is displayed for the user ID.

<p align="center">
  <img src="/images/screenShot.png" width="450"/>
</p>

# Sweet Reddit Approach
Sweet Reddit uses Dec. 2015 comments in JSON format. User graph is been processed in batch layer. Real-time data stream is synthetically generated. Real-time data stream affects user's final suggestion.

<p align="center">
  <img src="/images/pipeline.png" width="450"/>
</p>


## Data Synethesis
Real-time data stream is synthesized based on the Dec. 2015 user pool (2.8 Million distinct users), with random posts in the same pool. Post-Comment rate is 30, which is 3 times of average Reddit post-comment ratio. Higher post-comment ration leads to more complex graph structure, which is used to stress real time layer performance.

JSON message fields:
- url:          URL of the original post
- title:        title of the original post
- author        user ID
- created_utc:  timestamp of the comment in epoch second
- subreddit:    the sub-reddit the comment belongs to
- ups:          promotions
- name:         name of the comment
- id:           id of the comment
- subreddit_id: sub-reddit ID

~ 30 GB of historical data
~ 2.8 Million distinct users on Dec. 2016
~ 1300 messages streaming per second


## Data Ingestion
JSON messages were produced and consumed by python scripts using the kafka-python package from https://github.com/mumrah/kafka-python.git. Messages were published to a single topic with Spark Streaming as consumers. The batch job operates on the one month timing window.

## Batch Processing
Batch job has 4 tasks:
1. Construct user graph table and store in cassandra and redis
2. Build "find users by a post" table and save in cassandra and redis
3. Build "find posts by a user" table and save in cassandra and redis
4. Build "find title and body by url" table and save in cassandra only

Batch views were directly written into cassandra with the spark-cassandra connector, and written into redis as caching layer

sbt libarary dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1"
- "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

A full batch process was made in the case of needing to rebuild the entire batch view. Typically the incremental batch process is run daily from the cached folder on Amazon S3, the source of truth. 

## Real-time Processing
Four stream processes were performed for real-time views:

1. Find a list of posts that a new user commented on, in batch layer and real time layer
2. For all the posts found in step 1, find all users that commented on them, in batch layer and real time layer
3. Add new edges that between new user and the users found in step 2
4. Aggregate the edges into graph table in redis as real time layer user graph

Messages streamed into Spark Streaming with the spark-kafka connector
Real-time views were written into redis caching layer for fast edge update

sbt library dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1"
- "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
- "org.apache.spark" % "spark-streaming_2.10" % "1.2.0" % "provided"
- "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0"
  
## Database Schema
Tables:

1. find_post_by_user_realtime: table populated by Spark Streaming (real-time) representing real time posts of a user
2. find_post_by_user_batch:    table populated by Spark batch job representing batch layer posts of a user
3. find_user_by_post_realtime: table populated by Spark Streaming (real-time) representing real time users of a post
4. find_user_by_post_batch:    table populated by Spark batch job representing batch layer users of a post
5. user_graph_table_realtime:  table populated by Spark Streaming (real-time) representing real time user graph
6. user_graph_table_batch:     table populated by Spark batch job representing batch layer user graph

```
-- use user id to find post, clustering with create time
CREATE TABLE user_post_table(user text, created_utc bigint, url text, subreddit text, title text, year_month text, body text, PRIMARY KEY ((user), created_utc));
-- use post url to find user
CREATE TABLE post_user_table(url text, user text, created_utc bigint, subreddit text, title text, year_month text, body text, PRIMARY KEY ((url), user));
-- use user1 to find another user who has most post in common, batch layer
CREATE TABLE user_graph(user1 text, nCommonPosts int, user2 text, PRIMARY KEY ((user1), nCommonPosts)) WITH CLUSTERING ORDER BY (nCommonPosts DESC);
```

## Startup Protocol
1. Kafka server
2. Spark Streaming
3. Spark Streaming Kafka consumer
4. Kafka message producer
