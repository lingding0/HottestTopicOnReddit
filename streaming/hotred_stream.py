from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange

from cassandra.cluster import Cluster
from random import randint
import pprint

import time
import json

CASSANDRA_CLUSTER_IP_LIST = ['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com']
KEY_SPACE = 'hotred'

# Kafka streaming connection parameters
KAFKA_NODE1 = 'ec2-52-41-93-106.us-west-2.compute.amazonaws.com'
KAFKA_NODE2 = 'ec2-52-41-99-33.us-west-2.compute.amazonaws.com'
KAFKA_NODE3 = 'ec2-52-41-132-185.us-west-2.compute.amazonaws.com'

kafka_dns  = KAFKA_NODE1
kafka_port = "2181"

#sc = SparkContext("spark://ip-172-31-0-104:7077", appName="StreamingKafka")
sc = SparkContext("local[*]", appName="StreamingKafka")
# streaming batch interval of 5 sec first, and reduce later to 1 sec or lower
ssc = StreamingContext(sc, 60)
#ssc = StreamingContext.getOrCreate(checkpoint,
#                                   lambda: createContext(host, int(port), output))


#def ts2date(curTime):
#    return time.strftime("%Y-%m-%d", time.localtime(int(curTime)))


#st_news = session.prepare("INSERT INTO topnews (subreddit_id, created_utc, link_id, parent_id, name, body) VALUES (?,?,?,?,?,?) USING TTL 7776000") #let news live for 90 days in the database


#rdcluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
#rdsession = rdcluster.connect(KEY_SPACE)

def readUserPostsFromDB(user): # requires quite some DB access
    rdcluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
    rdsession = rdcluster.connect(KEY_SPACE)

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


def process(rdd):
    for comment in rdd.collect():
        primKey = comment[0] + str(randint(0,19))
        session.execute(st_news, (primKey, comment[1], comment[2], comment[3], comment[4], comment[5], ))
	#print ("pushing comment into cassandra: " + comment[0] + str(cnt))
        #session.execute(st_news, (data["created_utc"] + str(cnt), data["subreddit_id"], data["link_id"], data["parent_id"], data["name"], data["body"], ))


def getJson(comment):
    # comment[0]: kafka offset; comment[1]: payload string
    data = json.loads(comment[1])
           #        0                1                    2               3            4             5            6              7
    return (data['author'], data['created_utc'], data['subreddit'], data['id'], data['body'], data['score'], data['url'], data['title'])


def printRDD(rdd):
    if rdd:
        pp = pprint.PrettyPrinter(indent=4)
        for item in rdd.collect():
            pp.pprint(item)


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


def insert_graph(rdd):
    if rdd:
        cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
        session = cluster.connect(KEY_SPACE)
        graph_stmt = session.prepare("INSERT INTO user_graph_realtime (user1, nCommonPosts, user2) VALUES (?,?,?)")
        session.execute(graph_stmt, ('Leo', 15, 'David'))

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

# Kafka brokers
BROKER_LIST = KAFKA_NODE1 + ':9092,' + KAFKA_NODE2 + ':9092,' + KAFKA_NODE3 + ':9092'
kafkaParams = {"metadata.broker.list": BROKER_LIST}
kafkaStream = KafkaUtils.createDirectStream(ssc,  # stream context
                                              ["reddit"], # topics
                                              kafkaParams) # broker list, auto connect to each partition

jsonData  = kafkaStream.map(getJson)
jsonData.foreachRDD(lambda rdd: rdd.foreachPartition(insert_user_table))

# calculate user relationship graph delta and save into realtime graph
# (URL, user) tuple
post2user = jsonData.map(lambda x: (x[6], x[0]))

# realtime has 3 new edge sources
# 1. relationship between new posts that is in this micro-batch
newEdgesByNewPosts = post2user.join(post2user)\
                              .filter(lambda x: x[1][0] != x[1][1])\
                              .map(lambda x: (x[1][0], x[1][1]))\
                              .map(makeAscOrder)\
                              .map(lambda x: (x, 1))

# 2. new posts that has relationship with batch layer posts
newEdgesByBatchPosts = post2user.map(lambda x: x[1])\
                              .flatMap(readUserPostsFromDB)\
                              .map(makeAscOrder)\
                              .map(lambda x: (x, 1))

# 2. new posts that has relationship with realtime layer earlier posts
newEdgesByRtimePosts = post2user.map(lambda x: x[1])\
                              .flatMap(readUserPostsRealDB)\
                              .map(makeAscOrder)\
                              .map(lambda x: (x, 1))

# Union all type together and count new edges
allCreatedEdges = newEdgesByBatchPosts.union(newEdgesByNewPosts).union(newEdgesByRtimePosts)

newEdges        = allCreatedEdges.reduceByKey(lambda x, y: x+y)\
                                 .map(lambda x: (x[0][0], x[1], x[0][1]))

#newEdges  = post2user.map(lambda x: x[1])\
#                     .flatMap(readUserPostsFromDB)\
#                     .map(makeAscOrder)\
#                     .map(lambda x: (x, 1))\
#                     .reduceByKey(lambda x, y: x+y)\
#                     .map(lambda x: (x[0][0], x[1], x[0][1]))

#graph     = post2user.join(post2user)\                       # self join to find user relationship by posts
#                     .filter(lambda x: x[1][0] != x[1][1])\  # remove all self linked relationship
#                     .map(makeAscOrder)\                     # make to asc order by user name
#                     .distinct()\        # remove duplicated user pairs, because the relationship is mutual
#                     .map(lambda x: (x[1], 1))\              # ready tho count number of common edges
#                     .reduceByKey(lambda x, y: x+y)\         # count total number for every edge/relationship
#                     .map(lambda x: (x[0][0], x[1], x[0][1]))# flatten and ready to write table

#newEdges.foreachRDD(printRDD)
#newEdges.foreachRDD(insert_graph)
newEdges.foreachRDD(lambda rdd: rdd.foreachPartition(insert_graph))

ssc.start()
ssc.awaitTermination()



