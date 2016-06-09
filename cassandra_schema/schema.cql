CREATE KEYSPACE hotred WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};
USE hotred;

-- top reddit topic table
CREATE TABLE topnews(created_utc long, subreddit_id text, link_id text, parent_id text, name text, body text PRIMARY KEY (created_utc, subreddit_id));


