import json
import pymongo as pm
import sys
from Queue import *

try:
  connection = pm.MongoClient()
  db = connection['spark_data']
  data = db['data']
  hashtags = db['hashtags']
  mentions = db['mentions']
  co_occurring_ht = db['co_occurring_ht']
except:
  print "Cannot connect to the mongo client. Please check the port address"


def popularHashtag(n):
  try:
    total_hashtag = hashtags.find().sort("count", pm.DESCENDING).limit(n)
    for x in total_hashtag:
      print x["hashtag"]
  except:
    print "Error"

def popularMention(n):
  try:
    total_mention = mentions.find().sort("count", pm.DESCENDING).limit(n)
    for x in total_mention:
      print x["mention"]
  except:
    print "Error"

def count_hashtag(hash_tag):
  try:
    count_hashtag = hashtags.find({'hashtag':hash_tag}, {'count':1})
    print(count_hashtag[0]['count'])
  except:
    print "Error"

def count_mention(men):
  try:
    count_men = mentions.find({'mention':men},{'count':1})
    print(count_men[0]['count'])
  except:
    print "Error"

def co_occuring(n):
  try:
    total_hashtag = co_occurring_ht.find().sort("count", pm.DESCENDING).limit(n)
    hashtag_count = hashtags.find()
    hash_cooc = list()
    hash_dict = {}
    for x in total_hashtag:
      di = {}
      di['ht1'] = x['ht1']
      di['ht2'] = x['ht2']
      di['count'] = x['count']
      hash_cooc.append(di)
      # print json.dumps(di)
    for x in hash_cooc:
      hash_dict[x['ht1']] = hashtags.find_one({'hashtag': x['ht1']}, {'count' : 1, '_id' : 0})['count']
      hash_dict[x['ht2']] = hashtags.find_one({'hashtag': x['ht2']}, {'count' : 1, '_id' : 0})['count']
    final = {}
    final['hash_cooc'] = hash_cooc
    final['hash_dict'] = hash_dict
    print json.dumps(final)

  except:
    print "Error"

def co_occuring_hash(n, depth = 2):
  # try:
    BFS(n, depth)
  # except:
  #   print "Error"

  #--------------------------------------------------
# from queue import Queue
def BFS(root, depth_max):
  hash_cooc = list()
  hash_dict = {}
  q = Queue()
  checked = []
  q.put({'ht': root,'depth': 0})
  while not q.empty():
    v = q.get()
    # hash_dict[v['ht']] = hashtags.find_one({'hashtag': v['ht']}, {'count' : 1, '_id' : 0})['count']
    #if v == target:
    #   return True
    #el
    if v['ht'] not in checked and v['depth'] < depth_max:
      # print v
      edge_list = co_occurring_ht.find({ '$or': [ { 'ht1': v['ht']}, { 'ht2': v['ht']} ] },{'_id' : 0})
      
      # hash_cooc |= set(edge_list)
      for edge in edge_list:
        # print edge
        if edge not in hash_cooc:
          hash_cooc.append(edge)
        # if v not in checked:
        if (v['ht'] == edge['ht1']):
          q.put({'ht': edge['ht2'], 'depth' : v['depth'] + 1})
          # print v['depth'] + 1
        elif (v['ht'] == edge['ht2']):
          q.put({'ht': edge['ht1'], 'depth' : v['depth'] + 1})
          # print v['depth'] + 1
      checked.append(v['ht'])
    # hash_cooc = list(hash_cooc)
  for x in hash_cooc:
    hash_dict[x['ht1']] = hashtags.find_one({'hashtag': x['ht1']}, {'count' : 1, '_id' : 0})['count']
    hash_dict[x['ht2']] = hashtags.find_one({'hashtag': x['ht2']}, {'count' : 1, '_id' : 0})['count']
  final = {}
  final['hash_cooc'] = hash_cooc
  final['hash_dict'] = hash_dict
  print json.dumps(final)

option = sys.argv[1]
# print("You have entered option: " + str(option))

if(int(option) == 1):
  popularHashtag(int(sys.argv[2]))

if(int(option) == 2):
  popularMention(int(sys.argv[2]))

if(int(option) == 3):
  count_hashtag(sys.argv[2])

if(int(option) == 4):
  count_mention(sys.argv[2])

if(int(option) == 5):
  co_occuring(int(sys.argv[2]))

if(int(option) == 6):
  co_occuring_hash(str(sys.argv[2]), int(sys.argv[3]))


# print "HOLAAAA"