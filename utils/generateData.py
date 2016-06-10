import sys
import time
import datetime
from random import randint
import json

outFileName      = str(sys.argv[1])
throughputPerSec = int(sys.argv[2])

startTimeUTC = 1420070668
nHours = 0.01
nSecond = int(nHours * 60 * 60) # 12 hours of data

outFh = open(outFileName, 'w')

def crateOneCmt(timeOffset):
    oneCmt = {}
    oneCmt["created_utc"] = str(startTimeUTC + timeOffset)
    oneCmt["title"] = "Hello"
    oneCmt["subreddit_id"] = "t5_2s30g" + str(randint(0, 100))
    return oneCmt

def createCommentInOneSec(throughputPerSec, timeOffset):
    secData = []
    for i in range(0, throughputPerSec):
        secData.append(crateOneCmt(timeOffset))
    return secData
     
 
for i in range(0, nSecond):
    oneSecCmt = createCommentInOneSec(throughputPerSec, i)
    jsonStr = json.dumps(oneSecCmt)
    outFh.write(jsonStr + "\n")
   

outFh.close()

