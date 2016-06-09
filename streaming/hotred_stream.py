from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange

from cassandra.cluster import Cluster

import time
import json

# Kafka streaming connection parameters
kafka_dns  = "ec2-52-40-27-174.us-west-2.compute.amazonaws.com"
kafka_port = "2181"

sc = SparkContext("spark://ip-172-31-0-104:7077", appName="StreamingKafka")
# streaming batch interval of 5 sec first, and reduce later to 1 sec or lower
ssc = StreamingContext(sc, 5)
#ssc = StreamingContext.getOrCreate(checkpoint,
#                                   lambda: createContext(host, int(port), output))


#def ts2date(curTime):
#    return time.strftime("%Y-%m-%d", time.localtime(int(curTime)))


# connect to cassandra
cluster = Cluster(['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com'])
session = cluster.connect("hotred")
st_news = session.prepare("INSERT INTO topnews (subreddit_id, created_utc, link_id, parent_id, name, body) VALUES (?,?,?,?,?,?) USING TTL 7776000") #let news live for 90 days in the database


def process(rdd):
    cnt = 0
    for comment in rdd.collect():
        session.execute(st_news, (comment[0] + str(cnt), comment[1], comment[2], comment[3], comment[4], comment[5], ))
	print ("pushing comment into cassandra: " + comment[0] + str(cnt))
        #session.execute(st_news, (data["created_utc"] + str(cnt), data["subreddit_id"], data["link_id"], data["parent_id"], data["name"], data["body"], ))
        cnt += 1

#def aggToCassandra(top10):
#    if top10:
#        cascluster = Cluster(['52.89.47.199', '52.89.59.188', '52.88.228.95', '52.35.74.206'])
#        casSession = cascluster.connect('hotestRedditTopic')
#        for rec in top10:
#            casSession.execute('INSERT INTO recommend (subreddit_id, created_utc, name, body) VALUES (%s, %s, %s, %s)', (str(rec[1]), str(rec[0]), str(rec[5]), str(rec[6])))
#        casSession.shutdown()
#        cascluster.shutdown()

def getEdges(comment):
    type(comment)
    print ("comment = " + comment[1])
    data = json.loads(comment[1])
    subreddit_id = data['subreddit_id']
    link_id      = data['link_id']
    parent_id    = data['parent_id']
    body         = data['body']
    created_utc  = data['created_utc']
    name         = data['name']
    return (subreddit_id, created_utc, link_id, parent_id, name, body)

def findTopTopic(rdd):
    return rdd

# Kafka brokers
kafkaParams = {"metadata.broker.list": "ec2-52-40-27-174.us-west-2.compute.amazonaws.com:9092,ec2-52-37-195-19.us-west-2.compute.amazonaws.com:9092,ec2-52-39-242-87.us-west-2.compute.amazonaws.com:9092"}
kafkaStream = KafkaUtils.createDirectStream(ssc,  # stream context
                                              ["reddit"], # topics
                                              kafkaParams) # broker list, auto connect to each partition

graph = kafkaStream.map(getEdges)
graph.foreachRDD(process)

ssc.start()
ssc.awaitTermination()




