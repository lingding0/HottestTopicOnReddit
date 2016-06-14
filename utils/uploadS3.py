import boto
import boto.s3
import sys
from boto.s3.key import Key
import os

#file2S3 = "/home/ubuntu/HottestTopicOnReddit/data/comment_1month.txt"
file2S3 = "/home/ubuntu/HottestTopicOnReddit/data/comment_100_perSec"
#AWS_ACCESS_KEY_ID = ''
#AWS_SECRET_ACCESS_KEY = ''

s3_connection = boto.connect_s3(os.environ['AWS_ACCESS_KEY_ID'],
                                os.environ['AWS_SECRET_ACCESS_KEY'])
bucket = s3_connection.get_bucket('reddit-comments')  # bucket names must be unique
bucket.list()
#bucket = s3_connection.create_bucket('lding-reddit-comments1')  # bucket names must be unique
#bucket = s3_connection.get_bucket('lding-reddit-comments')
key = boto.s3.key.Key(bucket, 'myfile')
with open(file2S3) as f:
    key.send_file(f)


