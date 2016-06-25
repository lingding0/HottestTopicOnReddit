
ONLINE = False
ONLINE = True

from flask import render_template, request
from flask import jsonify 

if ONLINE:
    from app import app

from cassandra.cluster import Cluster
import json
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
    
    if dict1 == None and dict2 == None:
        return []

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
    return db.lrange(key, 0, -1)


def getRandomRecommendation():
    randomPosts = session.execute("SELECT * FROM user_post_table LIMIT 500")
    batchList = [(row.user, row.url, row.body) for row in randomPosts]

    recommendation = []
    for i in range(6):
        oneRec = batchList[randint(0, len(batchList))]
        oneRecommend = {}
        oneRecommend['otherUser'] = oneRec[0] + '*'
        oneRecommend['URL']       = oneRec[1]
        oneRecommend['title']     = oneRec[2]
        
        recommendation.append(oneRecommend)  

    return recommendation


def getPostOfUser(user):
    userPostsBatchLayer = session.execute("SELECT * FROM user_post_table WHERE user=%s LIMIT 5", parameters=[user])
    batchList = [(user, row.url, row.body) for row in userPostsBatchLayer]
    r5 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=5) # find URL by user in realtime layer
    r8 = redis.StrictRedis(host=REDIS_NODE, port=6379, db=8) # find title by URL in realtime layer
    urls = getRedisList(r5, user)
    rtimeList = []
    for url in urls:
        title = r8.get(url) # one on one mapping
        rtimeList.append((user, url, title))

    #pp.pprint(rtimeList)
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

    if len(fellowUsers) == 0:
        return getRandomRecommendation()

    fellowPosts = []
    for fellowUser in fellowUsers:
        fellowPosts.append(getPostOfUser(fellowUser)) # get a list of posts for the fellow user

    userPost    = getPostOfUser(user)       # get a list of posts for the login user
    userURLs    = [post[1] for post in userPost]

    recommendation = []
    for postGrp in fellowPosts:
        for post in postGrp:
            #pp.pprint(userPost)
            if post != None and post[1] not in userURLs: # compare URL only
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

def getPairString(user1, user2):
    pair = [user1, user2]
    pair.sort() # make uniq edge
    return ' '.join(pair) # concatanate with space


def get3layerNodes(user):
    layer1 = session.execute("SELECT * FROM user_graph WHERE user1=%s", parameters=[user])
    layer1dict = {} # find wight later on
    layer2dict = {}

    layer1users = []
    for row in layer1:
        layer1dict[row.user2] = row.ncommonposts
        layer1users.append(row.user2)

    
    l1tol2dict = {}
    for layer1user in layer1users:
        layer2 = session.execute("SELECT * FROM user_graph WHERE user1=%s", parameters=[layer1user])
        layer2users = []
        for row in layer2:
            layer2dict[getPairString(layer1user, row.user2)] = row.ncommonposts
            layer2users.append(row.user2)
        l1tol2dict[layer1user] = layer2users

    # build up json data structure
    # build up nodes
    assignedNodes = [] # one user only has one node assigned
    assignedNodes.append(user);

    jsonNodes = []
    centerNode = {"name":user,"group":0}
    jsonNodes.append(centerNode)

    # BFS layer by layer, layer1
    for layer1user in layer1users:
        if layer1user in assignedNodes:
            continue
        jsonNodes.append({"name":layer1user,"group":1})
        assignedNodes.append(layer1user)

    # BFS layer by layer, layer2
    for layer1user in layer1users:
        for layer2user in l1tol2dict[layer1user]:
            if layer2user in assignedNodes:
                continue
            jsonNodes.append({"name":layer2user,"group":2})
            assignedNodes.append(layer2user)


    # build up edges
    usedEdges = set()
    jsonEdges = []
    for layer1user in layer1users:
        usedEdge = getPairString(user, layer1user)
        if usedEdge in usedEdges:
            continue
        jsonEdges.append({"source":assignedNodes.index(user),"target":assignedNodes.index(layer1user),"value":layer1dict[layer1user]})
        usedEdges.add(usedEdge)

    # layer1 to layer2 edges
    for layer1user in layer1users:
        for layer2user in l1tol2dict[layer1user]:
            usedEdge = getPairString(layer1user, layer2user)
            if usedEdge in usedEdges:
                continue
            jsonEdges.append({"source":assignedNodes.index(layer1user),"target":assignedNodes.index(layer2user),"value":layer2dict[usedEdge]})
            usedEdges.add(usedEdge)

    return {"nodes":jsonNodes, "links":jsonEdges}


if ONLINE:
    @app.route('/')
    @app.route('/index')
    def index():
        return render_template("index.html")
    
    @app.route('/show')
    def showData():
        user = request.args.get("user")
        json_data = getRecommondationPost(user)
        graph_data = get3layerNodes(user)
        return render_template("showData.html", user=user, json_data=json_data, graph_data=graph_data)

#    @app.route('/graph')
#    def showGraph():
#        user = request.args.get("user")
#        graph_data = get3layerNodes(user)
#        #pp.pprint(graph_data)                
#        return render_template("showGraph.html", graph_data=graph_data)

else:
    recommend = getRecommondationPost('adkdkdkdk')
    #graph = get3layerNodes('kirkt')
    pp.pprint(recommend)                
    #pp.pprint(graph)                


