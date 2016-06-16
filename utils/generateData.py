import sys
import time
import datetime
from random import randint
import json
import copy

outFileName      = str(sys.argv[1])
throughputPerSec = int(sys.argv[2])

#startTimeUTC = 1420070668
startTimeUTC = 0
nHours = 1
nSecond = int(nHours * 60 * 60) # 12 hours of data

# extract submittion
def extractJsonToList(filename):
    result = []
    with open(filename) as json_file:
        for line in json_file:
            json_data = json.loads(line)
            result.append(json_data)
    return result

submittions = extractJsonToList("/home/ubuntu/Downloads/submittion_1000.txt")
comments    = extractJsonToList("/home/ubuntu/Downloads/comments_1000000.txt")

outFh = open(outFileName, 'w')

def crateOneCmt(timeOffset):
    # random select one coment for modification
    onePst = copy.deepcopy(submittions[randint(0, 9)])
    oneCmt = copy.deepcopy(comments[randint(0, 999999)])
    oneCmt["created_utc"] = str(startTimeUTC + timeOffset)
    oneCmt["title"] = onePst["title"]
    oneCmt["url"] = onePst["url"]
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

