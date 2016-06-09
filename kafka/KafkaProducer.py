import sys
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
from ConfigParser import SafeConfigParser
from CommentReader import CommentReader
from kafka import HashedPartitioner

from time import sleep
# generator based line by line processing in python
#import ijson
#filename = "md_traffic.json"
#with open(filename, 'r') as f:
#    objects = ijson.items(f, 'meta.view.columns.item')
#    columns = list(objects)

class KafkaLfProducer(object):
    def __init__(self, addr, conf_file):
        self.parser = SafeConfigParser()
        self.parser.read(conf_file)
        install_dir = self.parser.get('hotred_tool', 'INSTALL_DIR')
        zipdb_file  = self.parser.get('hotred_tool', 'ZIP_DB_FILE') 

        self.client = KafkaClient(addr)
        #self.producer = KeyedProducer(self.client, async=True, batch_send_every_n=500, batch_send=True)
        # HashedPartitioner is default
        self.producer = KeyedProducer(self.client, partitioner=HashedPartitioner)

#    def produce_msgs(self):
#        msg_cnt = 0
#        with open('./HottestTopicOnReddit/data/redditSampleJson.txt') as data_file:
#            for line in data_file:
#                data = json.loads(line)
#                pprint(data)
#                subreddit_id = data['subreddit_id']
#                msg = line
#
#                if msg_cnt % 500000 == 0:
#                    print "Sent " + str(msg_cnt) + " messages to Kafka"
#
#                # use subreddit ID as partition key for ensure the same topic along with
#                # its comments flow into the same channel and enter the same spark rdd
#                self.producer.send('reddit_stream', subreddit_id, msg)
#                msg_cnt += 1
#
#        print "Sent Total " + str(msg_cnt) + " messages to Kafka"
#	data_file.close()

    def produce_msgs(self):
        msg_cnt = 0
        line = "{"gilded":0,"author_flair_text":"Male","author_flair_css_class":"male","retrieved_on":1425124228,"ups":3,"subreddit_id":"t5_2s30g","edited":false,"controversiality":0,"parent_id":"","subreddit":"AskMen","body":"Message root post.","created_utc":"1420070668","downs":0,"score":3,"author":"TheDukeofEtown","archived":false,"distinguished":null,"id":"cnasd6x","score_hidden":false,"name":"t1_cnasd6x","link_id":"t3_2qyhmp"}"
	while (1):
	    data = json.loads(line)
            pprint(data)
            subreddit_id = data['subreddit_id']
            msg = line

            self.producer.send('reddit_stream', subreddit_id, msg)
            msg_cnt += 1
            print "Sent Total " + str(msg_cnt) + " messages to Kafka"
            sleep(1) # Time in seconds.


if __name__ == "__main__":
    args = sys.argv
    conf_file = str(args[1])
    ip_addr = str(args[2])
    prod = KafkaLfProducer(ip_addr, conf_file)
    prod.produce_msgs() 
