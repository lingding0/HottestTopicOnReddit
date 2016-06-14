import os
import boto

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

bucket_name = "reddit-comments"
#folder_name = "2015/"
folder_name = "2007/"
#file_name = "RC_2015-12"
file_name = "RC_2007-10"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

bucket = conn.get_bucket(bucket_name)
key = bucket.get_key(folder_name + file_name)

data = key.get_contents_as_string() # retures a byte stream 


