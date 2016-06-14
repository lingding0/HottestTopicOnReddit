import sys
from pyspark import SparkContext, SQLContext
#import ast
from cassandra.cluster import Cluster
from boto.s3.connection import S3Connection
import os
import boto
#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/config/')
#from constants import FILTER_SET
#from spark_constants import SPARK_ADDRESS
#from nltk import word_tokenize
from random import randint
import json
import copy

SPARK_ADDRESS = "spark://ip-172-31-0-104:7077"
CASSANDRA_CLUSTER_IP_LIST = ['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com']
KEY_SPACE = 'hotred'
GRAPH_NAME = 'userInCommon'
UPTBL_NAME = 'user_post_table'
PUTBL_NAME = 'post_user_table'

UPCOLUMN_ONE = 'author'
UPCOLUMN_TWO = 'created_utc'
UPCOLUMN_THREE = 'url'
UPCOLUMN_FOUR = 'subreddit'
UPCOLUMN_FIVE = 'title'
UPCOLUMN_SIX = 'year_month'
UPCOLUMN_SEVEN = 'body'

PUCOLUMN_ONE = 'url'
PUCOLUMN_TWO = 'author'
PUCOLUMN_THREE = 'created_utc'
PUCOLUMN_FOUR = 'subreddit'
PUCOLUMN_FIVE = 'title'
PUCOLUMN_SIX = 'year_month'
PUCOLUMN_SEVEN = 'body'

REPARTITION_SIZE = 3000
FROM_YEAR_MONTH = sys.argv[1]

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'default')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
RAW_JSON_REDDIT_BUCKET = "reddit-comments"
#FOLDER_NAME = "2015/"
#FOLDER_NAME = "2007/"
#FILE_NAME = "RC_2015-12"
#FILE_NAME = "RC_2007-10"

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
        cluster = Cluster(CASSANDRA_CLUSTER_IP_LIST)
        session = cluster.connect(KEY_SPACE)
        user_post_stmt = session.prepare("INSERT INTO user_post_table (user, created_utc, url, subreddit, title, year_month, body) VALUES (?,?,?,?,?,?,?)")
        post_user_stmt = session.prepare("INSERT INTO post_user_table (url, user, created_utc, subreddit, title, year_month, body) VALUES (?, ?, ?, ?, ?, ?, ?)")
        #user_post_stmt = session.prepare("INSERT INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}, {7}) VALUES (?, ?, ?, ?, ?, ?, ?)".format(UPTBL_NAME, UPCOLUMN_ONE, UPCOLUMN_TWO, UPCOLUMN_THREE, UPCOLUMN_FOUR, UPCOLUMN_FIVE, UPCOLUMN_SIX, UPCOLUMN_SEVEN))
        #post_user_stmt = session.prepare("INSERT INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}, {7}) VALUES (?, ?, ?, ?, ?, ?, ?)".format(PUTBL_NAME, PUCOLUMN_ONE, PUCOLUMN_TWO, PUCOLUMN_THREE, PUCOLUMN_FOUR, PUCOLUMN_FIVE, PUCOLUMN_SIX, PUCOLUMN_SEVEN))
        for item in partition:
                                                # author  created_utc            url     subreddit  id   year_month body
            #user_post_table = user_post_stmt.bind([item[0], int(item[2]) * 1000, item[10], item[3], item[9], item[1], item[5]])
            #post_user_table = post_user_stmt.bind([item[10], item[0], int(item[2]) * 1000, item[3], item[9], item[1], item[5]])
            #session.execute(user_post_stmt, ('abc', t, 'ddd', 'eeee', 'fffff', 'ggggg', 'hhhhhh'))
            session.execute(user_post_stmt, (item[0], long(item[2]) * 1000, item[10], item[3], item[9], item[1], item[5]))
            session.execute(post_user_stmt, (item[10], item[0], long(item[2]) * 1000, item[3], item[9], item[1], item[5]))
        session.shutdown()
        cluster.shutdown()

  

def main():

    sc = SparkContext(SPARK_ADDRESS, appName="RedditBatchLayer")
    #sc = SparkContext("local[*]", appName="RedditBatchLayer")
    bcURL = sc.broadcast(urlTitlePool)
    sqlContext = SQLContext(sc)

    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    #conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(RAW_JSON_REDDIT_BUCKET)
    #key = bucket.get_key(FOLDER_NAME + FILE_NAME)

    def addTitleURL(cmtTuple):
        onePst = bcURL.value[randint(0, 999)]
        return  cmtTuple + (onePst[0], onePst[1]) # adding random title and url
 
    #logFile = 's3a://reddit-comments/2007/RC_2007-10'
    #df = sqlContext.read.json(logFile)
    #df = sqlContext.jsonFile(logFile)
    #users_rdd = df.filter(df['author'] != '[deleted]') 
    #year = 2007
    #month = 12
    #users_row = users_rdd.map(lambda json: (json.author, '{0}_{1}'.format(year, month), json.created_utc, json.subreddit, json.id, json.body, json.score, json.ups, json.controversiality))\
    #                     .map(addTitleURL)
    #users_row.foreachPartition(insert_into_cassandra)

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
        df = sqlContext.read.json(logFile)
        df = sqlContext.jsonFile(logFile)
        users_rdd = df.filter(df['author'] != '[deleted]') 
                                               #   0                     1                        2                3            4          5          6          7              8           9 (title)   10(url)
        users_row = users_rdd.map(lambda json: (json.author, '{0}_{1}'.format(year, month), json.created_utc, json.subreddit, json.id, json.body, json.score, json.ups, json.controversiality))\
                             .map(addTitleURL)\
                             .repartition(REPARTITION_SIZE)
        users_row.foreachPartition(insert_into_cassandra)

    sc.stop()

if __name__ == '__main__':
    main()
