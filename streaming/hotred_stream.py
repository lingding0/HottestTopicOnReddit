from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster

from tdigest import TDigest

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

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rdd_date = rdd.map(lambda w: Row(houseId=str(json.loads(w)["houseId"]), date=str(ts2date(json.loads(w)['timestamp'])), zip=str(json.loads(w)["zip"]), power=str(float(json.loads(w)["readings"][0]['power']) + float(json.loads(w)["readings"][1]['power']))))
    rdd_aggr = rdd_date.map(lambda x: ((x.houseId, x.date, x.zip), x.power)).reduceByKey(lambda x, y: float(x)+float(y))
    comment = rdd_aggr.map(lambda x: {
        "houseId": x[0][0],
        "date": x[0][1],
        "zip": x[0][2],
        "power": x[1]
    })
    comment_df = sqlContext.createDataFrame(comment)
    comment_df.foreachPartition(aggToCassandra)


def aggToCassandra(top10):
    if top10:
        cascluster = Cluster(['52.89.47.199', '52.89.59.188', '52.88.228.95', '52.35.74.206'])
        casSession = cascluster.connect('hotestRedditTopic')
        for rec in top10:
            casSession.execute('INSERT INTO recommend (subreddit_id, created_utc, name, body) VALUES (%s, %s, %s, %s)', (str(rec[1]), str(rec[0]), str(rec[5]), str(rec[6])))
        casSession.shutdown()
        cascluster.shutdown()

def getEdges(comment):
    data = json.loads(comment)
    subreddit_id = data['subreddit_id']
    link_id      = data['link_id']
    parent_id    = data['parent_id']
    body         = data['body']
    created_utc  = data['created_utc']
    name         = data['name']
    return (created_utc, subreddit_id, link_id, parent_id, body, name, body)

def findTopTopic(rdd):
    return rdd

# direct stream is matching with Kafka 18 partitions - one on one match to make sure the comments
# always follow its topic earlier in the stream
nDStreams = 54; # 3 worker * 6 cores * 3 times overloading on thread
kafkaStreamHf = KafkaUtils.createDirectStream(ssc,  # stream context
                                              kafka_dns + ":" + kafka_port, # kafka zookeeper port
                                              "hotred_streaming",  # consumer group
                                              {"reddit": nDStreams}) # topic, with 18 partition
                                                                   # 3 workers, each has 6 cores

power_rt_hf = kafkaStreamHf.map(getEdges)
power_rt_hf.foreachRDD(process)

ssc.start()
ssc.awaitTermination()




