import sys
import time
import datetime
from random import randint
import json
import copy

outFileName      = str(sys.argv[1])
throughputPerSec = int(sys.argv[2])

smallFile = True

startTimeUTC = 1420070668
#startTimeUTC = 0
nHours = 0.2
nSecond = int(nHours * 60 * 60) # 12 hours of data

# extract submittion
def extractJsonToList(filename):
    result = []
    with open(filename) as json_file:
        for line in json_file:
            json_data = json.loads(line)
            result.append(json_data)
    return result

submittions = extractJsonToList("../data/submittion_10000.txt")

if (smallFile):
    pstRandRange = 3000-1 # 50 comments/topic
    cmtRandRange = 150000-1
    comments  = extractJsonToList("/home/ubuntu/Downloads/comments_1000000.txt")
    outFileName  = "../data/comment_small_10000_persec"
    throughputPerSec = 10000
else:
    pstRandRange = 1000-1
    cmtRandRange = 1000000-1
    comments  = extractJsonToList("/home/ubuntu/Downloads/RC_2007-10")
    
outFh = open(outFileName, 'w')

def crateOneCmt(timeOffset):
    # random select one coment for modification
    onePst = copy.deepcopy(submittions[randint(0, pstRandRange)])
    oneCmt = copy.deepcopy(comments[randint(0, cmtRandRange)])
    oneCmt["created_utc"] = str(startTimeUTC + timeOffset)
    oneCmt["title"]       = onePst["title"]
    oneCmt["url"]         = onePst["url"]
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

