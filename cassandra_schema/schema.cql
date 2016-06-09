CREATE KEYSPACE hotred WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};
USE hotred;

-- top reddit topic table
CREATE TABLE topnews(subreddit_id text, created_utc text, link_id text, parent_id text, name text, body text, PRIMARY KEY (subreddit_id, created_utc));


