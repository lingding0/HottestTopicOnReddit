import sys
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from ConfigParser import SafeConfigParser
from kafka import HashedPartitioner

from time import sleep
import json
import pprint
from threading import Thread


class KafkaProducer(object):
    def __init__(self, addr, conf_file):
        self.parser = SafeConfigParser()
        self.parser.read(conf_file)
        self.install_dir = self.parser.get('hotred_tool', 'INSTALL_DIR')
        self.zipdb_file  = self.parser.get('hotred_tool', 'ZIP_DB_FILE') 
        self.nHours      = float(self.parser.get('hotred_tool', 'DATA_LAST'))

        #self.client = KafkaClient(addr)
        self.client = SimpleClient(addr)
        #self.producer = KeyedProducer(self.client, async=True, batch_send_every_n=500, batch_send=True)
        # HashedPartitioner is default
        self.producer = KeyedProducer(self.client, partitioner=HashedPartitioner)

        self.timeCntInSec = long(0)  #global variable, to sync produer thread

    def produce_msgs(self, threadname):
        msg_cnt = 0
        secondCnt = long(0)
        nSecond = long(self.nHours * 60 * 60) # 12 hours of data
        pp = pprint.PrettyPrinter(indent=4)

        with open(self.install_dir + '/' + self.zipdb_file) as data_file:
            for line in data_file:
                data = json.loads(line)
                for i in range(len(data)):
                    user = bytes(data[i]['author'])
                    msg = json.dumps(data[i])

                    if msg_cnt % 5000 == 0:
                        print "Sent " + str(msg_cnt) + " messages to Kafka"

                    # use subreddit ID as partition key for ensure the same topic along with
                    # its comments flow into the same channel and enter the same spark rdd
                    self.producer.send('reddit', user, msg)
                    #pp.pprint(msg)
                    msg_cnt += 1
                    #print "Sent Total " + str(msg_cnt) + " messages to Kafka"
                
                secondCnt += 1
                if (secondCnt % nSecond == 0):
                    data_file.seek(0, 0) # rewind and start from beginning
                while (secondCnt >= self.timeCntInSec):
                    sleep(0.05) # look at other thread clock signal

        print "Sent Total " + str(msg_cnt) + " messages to Kafka"
	data_file.close()

    def syncClock(self, threadname):
        #global self.timeCntInSec
        while True:
            self.timeCntInSec += 1
            if (self.timeCntInSec % 60 == 0):
                print ("clock sig cnt: " + str(self.timeCntInSec) + "second")
            sleep(1)

    def syncProduceMsgs(self):
        clockTic     = Thread( target=self.syncClock, args=("syncedClockTic", ) )
        syncProducer = Thread( target=self.produce_msgs, args=("producerFollowsClock", ) )
        clockTic.start()
        syncProducer.start()
        clockTic.join()
        syncProducer.join() 

#    def produce_msgs(self):
#        msg_cnt = 0
#        pp = pprint.PrettyPrinter(indent=4)
#
#        line = '{"gilded":0,"author_flair_text":"Male","author_flair_css_class":"male","retrieved_on":1425124228,"ups":3,"subreddit_id":"t5_2s30g","edited":false,"controversiality":0,"parent_id":"","subreddit":"AskMen","body":"Message root post.","created_utc":"1420070668","downs":0,"score":3,"author":"TheDukeofEtown","archived":false,"distinguished":null,"id":"cnasd6x","score_hidden":false,"name":"t1_cnasd6x","link_id":"t3_2qyhmp"}'
#	while True:
#	    data = json.loads(line)
#            pp.pprint(data)
#            subreddit_id = bytes(data['subreddit_id'] + str(msg_cnt))
#            msg = line
#
#            self.producer.send('reddit', subreddit_id, msg)
#            msg_cnt += 1
#            print "Sent Total " + str(msg_cnt) + " messages to Kafka"
#            sleep(1) # Time in seconds.


if __name__ == "__main__":
    args = sys.argv
    conf_file = str(args[1])
    ip_addr = str(args[2])
    prod = KafkaProducer(ip_addr, conf_file)
    prod.syncProduceMsgs() 
