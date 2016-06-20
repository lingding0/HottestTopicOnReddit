
#ONLINE = False
ONLINE = True

from flask import render_template, request
from flask import jsonify 

if ONLINE:
    from app import app

from cassandra.cluster import Cluster
import Queue as Q  # ver. < 3.0
import redis
from random import randint
import pprint

pp = pprint.PrettyPrinter(indent=4)

N_TOP_USER = 6

#connect to cassandra
cluster = Cluster(['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com'])
session = cluster.connect("hotred")

# redis port address
REDIS_NODE = "ec2-52-40-80-40.us-west-2.compute.amazonaws.com"

session.default_fetch_size = None #turn off paging to allow IN () ORDER BY queries, since only a few records are SELECTed anyway

def addPriorityQ(q, weight, user):
    if q.qsize() < N_TOP_USER:
        q.put((weight, user))
    else:
        minInQ = q.get()
        if minInQ[0] < weight:
            q.put((weight, user))
        else:
            q.put(minInQ)

def getTopAccUsers(dict1, dict2):
    
    q = Q.PriorityQueue()
    d = {}

    if dict1 != None:
        for key, value in dict1.iteritems():
            if (dict2 == None or key not in dict2.keys()): # combine real time layer and batch layer
                d[key] = dict1[key]
            else:
                d[key] = dict1[key] + dict2[key]
            addPriorityQ(q, d[key], key)

    if dict2 != None:
        for key, value in dict2.iteritems():
            if (dict1 == None or key not in dict1.keys()):
                d[key] = dict2[key]
            addPriorityQ(q, d[key], key)

    n_max = []
    while not q.empty():
       n_max.append(q.get()[1])
    return n_max[::-1] # order from high to low


def getRedisList(db, key):
    value = db.get(key)
    if value == None:
        return []
    else:
        return value.split(' ')


def getPostOfUser(user):
    userPostsBatchLayer = session.execute("SELECT * FROM user_post_table WHERE user=%s LIMIT 5", parameters=[user])
    batchList = [(user, row.url, row.body) for row in userPostsBatchLayer]
    r5 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=5) # find URL by user in realtime layer
    r8 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=8) # find title by URL in realtime layer
    urls = getRedisList(r5, user)
    rtimeList = []
    for url in urls:
        title = getRedisList(r8, url)
        rtimeList.append((user, url, title))
    postList = list(set(batchList).union(set(rtimeList[:5])))
    return postList


def getStrongestFellowUser(user):
    strongestBatchLayer = session.execute("SELECT * FROM user_graph WHERE user1=%s LIMIT 6", parameters=[user])
    #strongestRtimeLayer = session.execute("SELECT * FROM user_graph_realtime WHERE user1=%s LIMIT 10", parameters=[user])
    
    batchTable = {}
    for row in strongestBatchLayer:
        batchTable[row.user2] = row.ncommonposts

    r7 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=7) # to find all user1 -> [user2, user3...]
    r4 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=4) # to find user1-user2 pair with edge
    endUsers = getRedisList(r7, user)
    rtimeTable = {}
    for endUser in endUsers:
        rtimeTable[endUser] = r4.get(user + ' ' + endUser)

    #pp.pprint(batchTable)
    #pp.pprint(rtimeTable)
            
    return getTopAccUsers(batchTable, rtimeTable)


def getRecommondationPost(user):
    fellowUsers = getStrongestFellowUser(user) # top N of the fellow users
    fellowPosts = []
    for fellowUser in fellowUsers:
        fellowPosts.append(getPostOfUser(fellowUser)) # get a list of posts for the fellow user
    userPost    = getPostOfUser(user)       # get a list of posts for the login user

    recommendation = []
    for postGrp in fellowPosts:
        for post in postGrp:
            if post[1] not in userPost[1]: # compare URL only
                 oneRecommend = {}
                 oneRecommend['otherUser'] = post[0]
                 oneRecommend['URL']       = post[1]
                 oneRecommend['title']     = post[2]
            
                 recommendation.append(oneRecommend)  
                 break
    
    return recommendation


def _get_data(time):
    #get a few of the latest trades for the user
    latest_topic = session.execute("SELECT * FROM topnews WHERE subreddit_id=%s", parameters=[time])

    return latest_topic

def _get_json_data(time):
    latest_topic = _get_data(time)
    topic_json = [{"subreddit_id": row.subreddit_id, "created_utc": row.created_utc, "body": row.body, "link_id": row.link_id, "name": row.name, "parent_id": row.parent_id} for row in latest_topic]
    return jsonify(latest_topic = topic_json)

def getDummy():
    json_data = []
    for i in range(4):
        dummy={}
        dummy['URL'] = 'URLakdk' + str(i)
        dummy['title'] = 'titleldldldld' + str(i)
        dummy['otherUser'] = 'otherUserkdkdkdkd' + str(i)
        json_data.append(dummy)
    return json_data


#json_data = getRecommondationPost('S7evyn')
#pp.pprint(json_data)                

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/show')
def showData():
    user = request.args.get("user")
    json_data = getRecommondationPost(user)
    return render_template("showData.html", user=user, json_data=json_data)

