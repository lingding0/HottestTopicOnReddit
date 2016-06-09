import linecache
import random
import json
import subprocess
from pprint import pprint

#class CommentConfig(object):
#    def __init__(self, sCommentId, commentId, sDataDir, zipCode, scaleFactor, numMeters, numLfRecs, numHfRecs):
#        self.sCommentId    = sCommentId
#        self.commentId     = commentId
#        self.sDataDir    = sDataDir
#        self.zipCode     = zipCode
#        self.scaleFactor = scaleFactor
#        self.numLfRecs   = numLfRecs
#        self.numHfRecs   = numHfRecs
#        self.numMeters   = numMeters

class CommentReader(object):

    def __init__(self, startCommentId, endCommentId, dataDir, redditComments):
        self.dataDir      = dataDir
        self.availComment   = set(range(startCommentId, endCommentId+1))


    def fileLen(self, fname):
        p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        result, err = p.communicate()

        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0])

    def commentSentDone(self):
        return len(self.availComment) == 0

    def getRecord(self):
        if (len(self.availComment) > 0):
            commentId = random.sample(self.availComment, 1)[0]
        else:
## TODO: Raise exception for this
            return None

    with open('/home/leo/HottestTopicOnReddit/data/redditSampleJson.txt') as data_file:
        for line in data_file:
            data = json.loads(line)
            pprint(data)

    return (data['subreddit_id'], data);

