import sys
from pyspark import SparkContext, SQLContext
from cassandra.cluster import Cluster
from boto.s3.connection import S3Connection
import os
import boto
from random import randint
import json
import copy
import redis

smallBatch = True
#smallBatch = False

SPARK_ADDRESS = "spark://ip-172-31-0-104:7077"
REDIS_NODE = "ec2-52-40-80-40.us-west-2.compute.amazonaws.com"

USE_CASSANDRA = True
USE_REDIS = True
#CASSANDRA_NODE1 = os.getenv('CASSANDRA_NODE1', 'default')
#CASSANDRA_NODE2 = os.getenv('CASSANDRA_NODE2', 'default')
#CASSANDRA_NODE3 = os.getenv('CASSANDRA_NODE3', 'default')
CASSANDRA_NODE1 = 'ec2-52-41-2-110.us-west-2.compute.amazonaws.com'
CASSANDRA_NODE2 = 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com'
CASSANDRA_NODE3 = 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com'
CASSANDRA_CLUSTER_IP_LIST = [CASSANDRA_NODE1, CASSANDRA_NODE2, CASSANDRA_NODE3]

KEY_SPACE = 'hotred'
GRAPH_NAME = 'userInCommon'
UPTBL_NAME = 'user_post_table'
PUTBL_NAME = 'post_user_table'

#REPARTITION_SIZE = 3000 # repartition only when data is skewed accross workers, example: one worker has 80% of data
REPARTITION_SIZE = 500
FROM_YEAR_MONTH = sys.argv[1]

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'default')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
RAW_JSON_REDDIT_BUCKET = "reddit-comments"


def agg2Redis(db, key, value):
    oldValue = db.get(key);
    if (oldValue == None):
        db.set(key, value.encode('utf-8'))
    else:
        db.set(key, oldValue + ' ' + value.encode('utf-8'))


def extractJsonToList(filename):
    result = []
    with open(filename) as json_file:
        for line in json_file:
            json_data = json.loads(line)
            result.append(json_data)
    return result

submittions = extractJsonToList("/home/ubuntu/HottestTopicOnReddit/data/submittion_1000.txt")
urlTitlePool = [(item['title'], item['url']) for item in submittions]

def insert_into_cassandra(partition):         
    if partition:
        if (USE_REDIS):
            r1 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=1) # find post by user on batch layer
            r2 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=2) # find user by post on batch layer

        if (USE_CASSANDRA):
            cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
            session = cluster.connect(KEY_SPACE)
            user_post_stmt = session.prepare("INSERT INTO user_post_table (user, created_utc, url, subreddit, title, year_month, body) VALUES (?,?,?,?,?,?,?)")
            post_user_stmt = session.prepare("INSERT INTO post_user_table (url, user, created_utc, subreddit, title, year_month, body) VALUES (?, ?, ?, ?, ?, ?, ?)")
        for item in partition:
            if (USE_REDIS):
                agg2Redis(r1, item[0], item[10])
                agg2Redis(r2, item[10], item[0])
            if (USE_CASSANDRA):
                                                # author  created_utc            url     subreddit  id   year_month body
                session.execute(user_post_stmt, (item[0], long(item[2]) * 1000, item[10], item[3], item[9], item[1], item[5]))
                session.execute(post_user_stmt, (item[10], item[0], long(item[2]) * 1000, item[3], item[9], item[1], item[5]))
        if (USE_CASSANDRA):
            session.shutdown()
            cluster.shutdown()


def insert_graph(partition):         
    if partition:
        if (USE_REDIS):
            r0 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=0) # graph on batch layer
        if (USE_CASSANDRA):
            cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
            session = cluster.connect(KEY_SPACE)
            graph_stmt = session.prepare("INSERT INTO user_graph (user1, nCommonPosts, user2) VALUES (?,?,?)")
        
        for item in partition:
            if (USE_REDIS):
                r0.set(item[0], str(item[1]) + ' ' + str(item[2]))
                r0.set(item[2], str(item[1]) + ' ' + str(item[0]))
            if (USE_CASSANDRA):
                session.execute(graph_stmt, (item[0], int(item[1]), item[2]))
                session.execute(graph_stmt, (item[2], int(item[1]), item[0]))
        if (USE_CASSANDRA):
            session.shutdown()
            cluster.shutdown()


def makeAscOrder(keyValuesPair):
    if (keyValuesPair[1][0] > keyValuesPair[1][1]):
        return (keyValuesPair[0], (keyValuesPair[1][1], keyValuesPair[1][0]))
    else:
        return keyValuesPair

def main():

    sc = SparkContext(SPARK_ADDRESS, appName="RedditBatchLayer")
    #sc = SparkContext("local[*]", appName="RedditBatchLayer")
    bcURL = sc.broadcast(urlTitlePool)
    sqlContext = SQLContext(sc)

    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    #conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(RAW_JSON_REDDIT_BUCKET)

    def addTitleURL(cmtTuple):
        onePst = bcURL.value[randint(0, 999)]
        return  cmtTuple + (onePst[0], onePst[1]) # adding title and url


    if (smallBatch): 
        logFile = 's3a://reddit-comments/2007/RC_2007-10'
        #df = sqlContext.read.json(logFile)
        df = sqlContext.jsonFile(logFile)
        users_rdd = df.filter(df['author'] != '[deleted]') 
        year = 2007
        month = 12
        users_row = users_rdd.map(lambda json: (json.author, '{0}_{1}'.format(year, month), json.created_utc, json.subreddit, json.id, json.body, json.score, json.ups, json.controversiality))\
                             .map(addTitleURL)
                             #.repartition(REPARTITION_SIZE)
        users_row.foreachPartition(insert_into_cassandra)

        # calculate user relationship graph
        # (URL, user) tuple
        post2user = users_row.map(lambda x: (x[10], x[0]))
        #graph     = post2user.join(post2user)\                       # self join to find user relationship by posts
        #                     .filter(lambda x: x[1][0] != x[1][1])\  # remove all self linked relationship
        #                     .map(makeAscOrder)\                     # make to asc order by user name
        #                     .distinct()\        # remove duplicated user pairs, because the relationship is mutual
        #                     .map(lambda x: (x[1], 1))\              # ready tho count number of common edges
        #                     .reduceByKey(lambda x, y: x+y)\         # count total number for every edge/relationship
        #                     .map(lambda x: (x[0][0], x[1], x[0][1]))# flatten and ready to write table
        graph     = post2user.join(post2user)\
                             .filter(lambda x: x[1][0] != x[1][1])\
                             .map(makeAscOrder)\
                             .distinct()\
                             .map(lambda x: (x[1], 1))\
                             .reduceByKey(lambda x, y: x+y)\
                             .map(lambda x: (x[0][0], x[1], x[0][1]))
        graph.foreachPartition(insert_graph)

    else:

        for key in bucket.list():
            if '-' not in key.name.encode('utf-8'): # filter out folders and _SUCCESS
                continue
            logFile = 's3a://{0}/{1}'.format(RAW_JSON_REDDIT_BUCKET, key.name.encode('utf-8'))
            year = logFile.split('-')[1][-4:] 
            month = logFile.split('-')[2]
            from_year = FROM_YEAR_MONTH.split('_')[0]
            from_month = FROM_YEAR_MONTH.split('_')[1]
            if int(year) < int(from_year) or (int(year) == int(from_year) and int(month) < int(from_month)):
                continue
            #df = sqlContext.read.json(logFile)
            df = sqlContext.jsonFile(logFile)
            users_rdd = df.filter(df['author'] != '[deleted]') 
                                                   #   0                     1                        2                3            4          5          6          7              8           9 (title)   10(url)
            users_row = users_rdd.map(lambda json: (json.author, '{0}_{1}'.format(year, month), json.created_utc, json.subreddit, json.id, json.body, json.score, json.ups, json.controversiality))\
                                 .map(addTitleURL)
                                 #.repartition(REPARTITION_SIZE)
            users_row.foreachPartition(insert_into_cassandra)

            # calculate user relationship graph
            # (URL, user) tuple
            post2user = users_row.map(lambda x: (x[10], x[0]))
            #graph     = post2user.join(post2user)\                       # self join to find user relationship by posts
            #                     .filter(lambda x: x[1][0] != x[1][1])\  # remove all self linked relationship
            #                     .map(makeAscOrder)\                     # make to asc order by user name
            #                     .distinct()\        # remove duplicated user pairs, because the relationship is mutual
            #                     .map(lambda x: (x[1], 1))\              # ready tho count number of common edges
            #                     .reduceByKey(lambda x, y: x+y)\         # count total number for every edge/relationship
            #                     .map(lambda x: (x[0][0], x[1], x[0][1]))# flatten and ready to write table
            graph     = post2user.join(post2user)\
                                 .filter(lambda x: x[1][0] != x[1][1])\
                                 .map(makeAscOrder)\
                                 .distinct()\
                                 .map(lambda x: (x[1], 1))\
                                 .reduceByKey(lambda x, y: x+y)\
                                 .map(lambda x: (x[0][0], x[1], x[0][1]))
                                 #.repartition(REPARTITION_SIZE)
            graph.foreachPartition(insert_graph)

    sc.stop()

if __name__ == '__main__':
    main()
