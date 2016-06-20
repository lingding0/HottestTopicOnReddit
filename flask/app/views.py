from flask import render_template, request
from flask import jsonify 
from app import app
from cassandra.cluster import Cluster
import redis

#connect to cassandra
cluster = Cluster(['ec2-52-41-2-110.us-west-2.compute.amazonaws.com', 'ec2-52-32-133-95.us-west-2.compute.amazonaws.com', 'ec2-52-34-129-5.us-west-2.compute.amazonaws.com'])
session = cluster.connect("hotred")

# redis port address
REDIS_NODE = "ec2-52-40-80-40.us-west-2.compute.amazonaws.com"

session.default_fetch_size = None #turn off paging to allow IN () ORDER BY queries, since only a few records are SELECTed anyway

def getTopAccUser(dict1, dict2):
    d = {}
    maxValue = -1;
    maxKey = ''
    for key, value in dict1.iteritems():
        if (dict2[key] == None):
            d[key] = dict1[key]
        else:
            d[key] = dict1[key] + dict2[key]
        if (d[key] > maxValue):
            maxValue = d[key]
            maxKey   = key

    for key, value in dict2.iteritems():
        if (dict1[key] == None):
            d[key] = dict2[key]
        if (d[key] > maxValue):
            maxValue = d[key]
            maxKey   = key

    return maxKey


def getRedisList(db, key):
    value = db.get(key)
    if value == None:
        return []
    else:
        return value.split(' ')


def getPostOfUser(user):
    userPostsBatchLayer = session.execute("SELECT * FROM user_post_table WHERE user=%s", parameters=[user])
    batchList = [row.url for row in userPostsBatchLayer]
    r5 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=5)
    rtimePost = getRedisList(r5, user)
    return list(set(batchList) + set(rtimeList))


def getStrongestFellowUser(user):
    strongestBatchLayer = session.execute("SELECT * FROM user_graph WHERE user1=%s LIMIT 10", parameters=[user])
    #strongestRtimeLayer = session.execute("SELECT * FROM user_graph_realtime WHERE user1=%s LIMIT 10", parameters=[user])
    
    batchTable = {}
    for row in strongestBatchLayer:
        batchTable[row.usr2] = row.ncommonposts

    r7 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=7) # to find all user1 -> [user2, user3...]
    r4 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=4) # to find user1-user2 pair with edge
    endUsers = getRedisList(r7, user)
    rtimeTable = {}
    for endUser in endUsers:
        rtimeTable[user + ' ' + endUser] = r4.get(user + ' ' + endUser)
        
    return getTopAccUser(batchList, rtimeList)


def getRecommondationPost():
    user = request.args.get("user")
    fellowUser = getStrongestFellowUser(user)
    fellowPost = getPostOfUser(fellowUser)
    userPost   = getPostOfUser(user)
    
    recommendation = list(set(fellowPost) - set(userPost))
    return recommendation[0]


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


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/show')
def showData():
    json_data = getDummy()
    user = request.args.get("user")
    return render_template("showData.html", user=user, json_data=json_data)

