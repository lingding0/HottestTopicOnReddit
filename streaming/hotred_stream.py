from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange

import redis
from cassandra.cluster import Cluster
from random import randint
import pprint
from boto.s3.connection import S3Connection

import os
import time
import json

USE_CASSANDRA = False
USE_REDIS = True
PRE_LOAD_GRAPH = True
USE_CHPT = False

SMALL_STREAM = True

AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID', 'default')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
#CHECKPOINT_DIR = 's3a://lding-checkpoint/'

HDFS_NAME_NODE = 'ec2-52-36-19-145.us-west-2.compute.amazonaws.com'
CHECKPOINT_DIR = 'hdfs://' + HDFS_NAME_NODE + ':9000/usr/checkpoint/'

conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

if (SMALL_STREAM):
    KAFKA_TOPIC = 'reddit1'
else:
    KAFKA_TOPIC = 'reddit'

REDIS_NODE = "ec2-52-40-80-40.us-west-2.compute.amazonaws.com"

CASSANDRA_CLUSTER_IP_LIST = ['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com']
KEY_SPACE = 'hotred'

# Kafka streaming connection parameters
KAFKA_NODE1 = 'ec2-52-41-2-110.us-west-2.compute.amazonaws.com'
KAFKA_NODE2 = 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com'
KAFKA_NODE3 = 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com'

SPARK_MASTER = 'spark://ip-172-31-0-116:7077'

kafka_dns  = KAFKA_NODE1
kafka_port = "2181"


pp = pprint.PrettyPrinter(indent=4)

def flushRedis(dbID):
    r = redis.StrictRedis(host=REDIS_NODE, port=6379, db=dbID)
    r.flushdb() # clear

if USE_REDIS:
    for dbID in range(4,10):
        flushRedis(dbID) # clear all realtime table


def readBatchLayer(dbID):
    keyValuePairs = {}
    r = redis.StrictRedis(host=REDIS_NODE, port=6379, db=dbID)
    for key in r.keys():
        valueList = r.lrange(key, 0, -1)
        if (valueList == None):
            continue
        keyValuePairs[key] = valueList
    return keyValuePairs

if PRE_LOAD_GRAPH:
    batchUserPostDict = readBatchLayer(1) # read user - post table
    batchPostUserDict = readBatchLayer(2) # read post - user table


def readUserPostsFromRDD(user, batchUserPostDict, batchPostUserDict): # read from RDD, no DB access
    if user not in batchUserPostDict.keys():
        return ()
    postList = batchUserPostDict[user]
    newEdges = []
    for post in postList:
        if post not in batchPostUserDict.keys():
            continue
        for fellowUser in batchPostUserDict[post]:
            newEdges.append((user, fellowUser))

    return tuple(newEdges)
      

def readUserPostsFromDB(user): # requires quite some DB access
    fellowUsers = []
    newEdges = []
    if (USE_REDIS):
            r1 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=1) # find post by user on batch layer
            r2 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=2) # find user by post on batch layer
            postList = r1.lrange(user, 0, -1)
            if postList == None:
                return ()
            for postID in postList:
                userSubGrp = r2.lrange(postID, 0, -1)
                if (userSubGrp == None):
                    continue
                if user in userSubGrp:
                    continue # only add fellow users who are not commented the post before
                fellowUsers.append(userSubGrp)

            for userGrp in fellowUsers:
                for fellowUser in userGrp:
                    newEdges.append((user, fellowUser))

              
    if (USE_CASSANDRA):
        allUserPosts = rdsession.execute("SELECT * FROM user_post_table WHERE user=%s", parameters=[user])
        postList = [row.url for row in allUserPosts]
        fellowUsers = []
        for postID in range(len(postList)):
            userSubGrp = rdsession.execute("SELECT * FROM post_user_table WHERE url=%s", parameters=[postList[postID]])
            subGrpList = [row.user for row in userSubGrp]
            if (user in subGrpList):
                continue # only add fellow users who are not commented the post before
            fellowUsers.append(subGrpList)

        newEdges = []
        for userGrpID in range(len(fellowUsers)):
            for fellowUserID in range(len(fellowUsers[userGrpID])):
                newEdges.append((user, fellowUsers[userGrpID][fellowUserID]))

        rdsession.shutdown()
        rdcluster.shutdown()

    return tuple(newEdges)


def readUserPostsRealDB(user): # requires quite some realtime DB access
    fellowUsers = []
    newEdges = []
    if (USE_REDIS):
            r5 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=5) # find post by user on realtime layer
            r6 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=6) # find user by post on realtime layer
            postList = r5.lrange(user, 0, -1)
            if postList == None:
                return ()

            for post in postList:
                userSubGrp = r6.lrange(post, 0, -1)
                if userSubGrp == None:
                    continue
                if user in userSubGrp:
                    continue # only add fellow users who are not commented the post before
                fellowUsers.append(userSubGrp)

            for userGrp in fellowUsers:
                for fellowUser in userGrp:
                    newEdges.append((user, fellowUser))


    if (USE_CASSANDRA):
        rdcluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
        rdsession = rdcluster.connect(KEY_SPACE)

        allUserPosts = rdsession.execute("SELECT * FROM user_post_table_realtime WHERE user=%s", parameters=[user])
        postList = [row.url for row in allUserPosts]
        fellowUsers = []
        for postID in range(len(postList)):
            userSubGrp = rdsession.execute("SELECT * FROM post_user_table_realtime WHERE url=%s", parameters=[postList[postID]])
            subGrpList = [row.user for row in userSubGrp]
            if (user in subGrpList):
                continue # only add fellow users who are not commented the post before
            fellowUsers.append(subGrpList)

        newEdges = []
        for userGrpID in range(len(fellowUsers)):
            for fellowUserID in range(len(fellowUsers[userGrpID])):
                newEdges.append((user, fellowUsers[userGrpID][fellowUserID]))

        rdsession.shutdown()
        rdcluster.shutdown()

    return tuple(newEdges)


def getJson(comment):
    # comment[0]: kafka offset; comment[1]: payload string
    kafka_offset = comment[0]
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(kafka_offset)

    data = json.loads(comment[1])
           #        0                1                    2               3            4             5            6              7
    #return (data['author'], data['created_utc'], data['subreddit'], data['id'], data['body'], data['score'], data['url'], data['title'])
    return (data['author'], data['created_utc'], data['subreddit'], data['id'], data['body'], str(kafka_offset), data['url'], data['title'])


def insert_user_table(rdd):
    if rdd:
        cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
        session = cluster.connect(KEY_SPACE)
        user_post_stmt = session.prepare("INSERT INTO user_post_table_realtime (user, created_utc, url, subreddit, title, body) VALUES (?,?,?,?,?,?)")
        post_user_stmt = session.prepare("INSERT INTO post_user_table_realtime (url, user, created_utc, subreddit, title, body) VALUES (?,?,?,?,?,?)")

        for item in rdd:
            session.execute(user_post_stmt, (item[0], long(item[1]) * 1000, item[6], item[2], item[7], item[4]))
            session.execute(post_user_stmt, (item[6], item[0], long(item[1]) * 1000, item[2], item[7], item[4]))

        session.shutdown()
        cluster.shutdown()


def agg2graph(db, key1, value, key2):
    oldValue = db.get(key1 + ' ' + key2);
    if (oldValue == None):
        db.set(key1 + ' ' + key2, str(value))
    else:
        db.set(key1 + ' ' + key2, str(int(oldValue) + int(value)))


def agg2Redis(db, key, value):
    db.rpush(key, value)


def insert_realtime_post_title(rdd):
    if rdd:
        if (USE_REDIS):
            # db 8: post-title readtime table
            r8 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=8) # write on realtime post-title table
            for item in rdd:
                r8.set(item[0], item[1].encode('utf-8')) # one one one mapping


def insert_realtime_post_user(rdd):
    if rdd:
        if (USE_REDIS):
            # db 5: user-post realtime table
            # db 6: post-user readtime table
            r5 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=5) # write on realtime user/post table
            r6 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=6) # write on realtime user/post table
            for item in rdd:
                agg2Redis(r5, item[1], item[0])
                agg2Redis(r6, item[0], item[1])


def insert_graph(rdd):
    if rdd:
        if (USE_REDIS):
            r4 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=4) # read modify write on realtime graph
            for item in rdd:
                agg2graph(r4, item[0], item[1], item[2])
                agg2graph(r4, item[2], item[1], item[0])


        if (USE_CASSANDRA):
            cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
            session = cluster.connect(KEY_SPACE)
            graph_stmt = session.prepare("INSERT INTO user_graph_realtime (user1, nCommonPosts, user2) VALUES (?,?,?)")

            for item in rdd:
                userPair = session.execute("SELECT * FROM user_graph_realtime WHERE user1=%s and user2=%s ALLOW FILTERING", parameters=[item[0], item[2]])
                if (userPair == None): # insert new entry into realtime graph
                    session.execute(graph_stmt, (item[0], int(item[1]), item[2]))
                    session.execute(graph_stmt, (item[2], int(item[1]), item[0]))
                else: # update entry in realtime graph
                    oldEdgeWeight = userPair.nCommonPosts
                    session.execute("UPDATE user_graph_realtime SET nCommonPosts=%d WHERE user1=%s and user2=%s ALLOW FILTERING", parameters=[int(item[1]) + int(oldEdgeWeight), item[0], item[2]])
                    session.execute("UPDATE user_graph_realtime SET nCommonPosts=%d WHERE user1=%s and user2=%s ALLOW FILTERING", parameters=[int(item[1]) + int(oldEdgeWeight), item[2], item[0]])

            session.shutdown()
            cluster.shutdown()


def makeAscOrder(keyValuesPair):
    if (keyValuesPair[0] > keyValuesPair[1]):
        return (keyValuesPair[1], keyValuesPair[0])
    else:
        return keyValuesPair



if PRE_LOAD_GRAPH:
    if USE_CHPT:
        def createContext():
            uBATCH_INTERVAL = 10
            sc = SparkContext(SPARK_MASTER, appName="StreamingKafka")
            sc.broadcast(batchUserPostDict)
            sc.broadcast(batchPostUserDict)
            #sc = SparkContext("local[*]", appName="StreamingKafka")
            # streaming batch interval of 5 sec first, and reduce later to 1 sec or lower
            ssc = StreamingContext(sc, uBATCH_INTERVAL)
            ssc.checkpoint(CHECKPOINT_DIR)   # set checkpoint directory in HDFS
            #ssc.checkpoint(10 * uBATCH_INTERVAL)
            return ssc

            ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, createContext)
            #ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR,
            #                                   lambda: createContext(host, int(port), output))
    else:    
        uBATCH_INTERVAL = 10
        sc = SparkContext(SPARK_MASTER, appName="StreamingKafka")
        sc.broadcast(batchUserPostDict)
        sc.broadcast(batchPostUserDict)
        ssc = StreamingContext(sc, uBATCH_INTERVAL)
        
else:
    uBATCH_INTERVAL = 10
    sc = SparkContext(SPARK_MASTER, appName="StreamingKafka")
    ssc = StreamingContext(sc, uBATCH_INTERVAL)
   

# Kafka brokers
BROKER_LIST = KAFKA_NODE1 + ':9092,' + KAFKA_NODE2 + ':9092,' + KAFKA_NODE3 + ':9092'
kafkaParams = {"metadata.broker.list": BROKER_LIST}
kafkaStream = KafkaUtils.createDirectStream(ssc,  # stream context
                                              [KAFKA_TOPIC], # topics
                                              kafkaParams) # broker list, auto connect to each partition

jsonData  = kafkaStream.map(getJson).filter(lambda x: x[0] != '[deleted]')
#jsonData.pprint()

#jsonData.foreachRDD(lambda rdd: rdd.foreachPartition(insert_user_table))
url2title = jsonData.map(lambda x: (x[6], x[7]))
url2title.foreachRDD(lambda rdd: rdd.foreachPartition(insert_realtime_post_title))

# calculate user relationship graph delta and save into realtime graph
# (URL, user) tuple
post2user = jsonData.map(lambda x: (x[6], x[0]))
post2user.cache()

#post2user.foreachRDD(lambda rdd: rdd.foreachPartition(insert_realtime_post_user))

#post2user.pprint()
# realtime has 3 new edge sources
# 1. relationship between new posts that is in this micro-batch
newEdgesByNewPosts = post2user.join(post2user)\
                              .filter(lambda x: x[1][0] != x[1][1])\
                              .map(lambda x: (x[1][0], x[1][1]))\
                              .map(makeAscOrder)\
                              .map(lambda x: (x, 1))

#newEdgesByNewPosts.pprint()

# 2. new posts that has relationship with batch layer posts
if PRE_LOAD_GRAPH:
    newEdgesByBatchPosts = post2user.map(lambda x: x[1])\
                                  .flatMap(lambda x: readUserPostsFromRDD(x, batchUserPostDict, batchPostUserDict))\
                                  .map(makeAscOrder)\
                                  .map(lambda x: (x, 1))
#else:
#    newEdgesByBatchPosts = post2user.map(lambda x: x[1])\
#                                  .flatMap(readUserPostsFromDB)\
#                                  .map(makeAscOrder)\
#                                  .map(lambda x: (x, 1))

#newEdgesByBatchPosts.pprint()

# 3. new posts that has relationship with realtime layer earlier posts
if PRE_LOAD_GRAPH:
    if USE_CHPT:
        def updateRealTimeGraph(newValues, runningCount):
            if runningCount is None:
               runningCount = 0
            return sum(newValues, runningCount)

        realtimeGraphNewEdges = newEdgesByNewPosts.updateStateByKey(updateRealTimeGraph)
    else:
        realtimeGraphNewEdges = newEdgesByNewPosts
        
#else:
#    newEdgesByRtimePosts = post2user.map(lambda x: x[1])\
#                                  .flatMap(readUserPostsRealDB)\
#                                  .map(makeAscOrder)\
#                                  .map(lambda x: (x, 1))


#newEdgesByRtimePosts.pprint()
# Union all type together and count new edges
#allCreatedEdges = newEdgesByNewPosts.union(newEdgesByRtimePosts)
if PRE_LOAD_GRAPH:
    allCreatedEdges = realtimeGraphNewEdges.union(newEdgesByBatchPosts)
else:
#    allCreatedEdges = newEdgesByNewPosts
    allCreatedEdges = newEdgesByNewPosts.union(newEdgesByRtimePosts).union(newEdgesByBatchPosts)
    # format ((user1, user2), commonPost)
#allCreatedEdges.pprint()

newEdges  = allCreatedEdges.reduceByKey(lambda x, y: x+y)\
                           .map(lambda x: (x[0][0], x[1], x[0][1]))

newEdges.foreachRDD(lambda rdd: rdd.foreachPartition(insert_graph))
#newEdges.pprint()

ssc.start()
ssc.awaitTermination()



