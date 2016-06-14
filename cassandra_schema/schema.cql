CREATE KEYSPACE hotred WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};
USE hotred;

-- top reddit topic table
CREATE TABLE topnews(subreddit_id text, created_utc text, link_id text, parent_id text, name text, body text, PRIMARY KEY (subreddit_id, created_utc));

-- use user id to find post, clustering with create time
CREATE TABLE user_topic_table(user text, created_utc text, url text, subreddit text, title text, year_month text, body text, PRIMARY KEY ((user), created_utc));


-- use post url to find user
CREATE TABLE topic_user_table(url text, user text, created_utc text, subreddit text, title text, year_month text, body text, PRIMARY KEY ((user), created_utc));

