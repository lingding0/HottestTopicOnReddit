import sys
from ConfigParser import SafeConfigParser
from kafka import KafkaProducer

from time import sleep
import json
import pprint
from threading import Thread

REWIND = True
SMALL_STREAM = True
FAST_INJECT = False
MSG_PER_SEC = 900

if (SMALL_STREAM):
    KAFKA_TOPIC = 'reddit1'
else:
    KAFKA_TOPIC = 'reddit'

class producer(object):
    def __init__(self, addr, conf_file):
        self.parser = SafeConfigParser()
        self.parser.read(conf_file)
        self.install_dir = self.parser.get('hotred_tool', 'INSTALL_DIR')
        self.zipdb_file  = self.parser.get('hotred_tool', 'ZIP_DB_FILE') 
        self.nHours      = float(self.parser.get('hotred_tool', 'DATA_LAST'))

        self.producer = KafkaProducer(bootstrap_servers=[addr + ":9092"],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                      acks=0,
                                      linger_ms=500)


        self.timeCntInSec = long(0)  #global variable, to sync produer thread

    def produce_msgs(self, threadname):
        msg_cnt = 0
        secondCnt = long(0)
        nSecond = long(self.nHours * 60 * 60) # 12 hours of data
        pp = pprint.PrettyPrinter(indent=4)

        if (SMALL_STREAM):
            streamInfileName = self.install_dir + '/data/comment_small_10000_persec'
        else:
            streamInfileName = self.install_dir + '/' + self.zipdb_file

        if FAST_INJECT:        
            lines = []
            with open(streamInfileName) as data_file:
                for line in data_file:
                    lines.append(line)
            #for i in range(len(data)):
            while True:
                oneSecData = json.loads(lines[secondCnt%nSecond])
                for oneMsg in oneSecData:
                    if msg_cnt % 5000 == 0:
                        print "Sent " + str(msg_cnt) + " messages to Kafka"

                    self.producer.send(KAFKA_TOPIC, oneMsg)
                    msg_cnt += 1
            
                secondCnt += 1
                while (secondCnt >= self.timeCntInSec):
                    sleep(0.05) # look at other thread clock signal
      
        else: 

            with open(streamInfileName) as data_file:
                for line in data_file:
                    data = json.loads(line)
                    for i in range(len(data)):
                        if i >= MSG_PER_SEC: # up to 10,000
                            break
                        if msg_cnt % 5000 == 0:
                            print "Sent " + str(msg_cnt) + " messages to Kafka"

                        self.producer.send(KAFKA_TOPIC, data[i])
                        msg_cnt += 1
                        #print "Sent Total " + str(msg_cnt) + " messages to Kafka"
                    
                    secondCnt += 1
                    if (REWIND and secondCnt % nSecond == 0):
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
                print ("clock sig cnt: " + str(self.timeCntInSec) + " seconds")
            sleep(1)

    def syncProduceMsgs(self):
        clockTic     = Thread( target=self.syncClock, args=("syncedClockTic", ) )
        syncProducer = Thread( target=self.produce_msgs, args=("producerFollowsClock", ) )
        clockTic.start()
        syncProducer.start()
        clockTic.join()
        syncProducer.join() 


if __name__ == "__main__":
    args = sys.argv
    conf_file = str(args[1])
    ip_addr = str(args[2])
    prod = producer(ip_addr, conf_file)
    prod.syncProduceMsgs() 
