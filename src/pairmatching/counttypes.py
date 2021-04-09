from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
from ..util import*

import functools
from pyspark.sql import DataFrame


SHORT_NAMES = {
    "prov:Entity": "ent",
    "prov:Activity": "act",
    "prov:Generation": "gen",
    "prov:Usage": "usd",
    "prov:Communication": "wib",
    "prov:Start": "wsb",
    "prov:End": "web",
    "prov:Invalidation": "inv",
    "prov:Derivation": "der",
    "prov:Agent": "agt",
    "prov:Attribution": "att",
    "prov:Association": "waw",
    "prov:Delegation": "del",
    "prov:Influence": "inf",
    "prov:Alternate": "alt",
    "prov:Specialization": "spe",
    "prov:Mention": "men",
    "prov:Membership": "mem",
}

#this is the general function for all type counting
def typesCountPairWise(g,lvl,primitive,rmDuplicates):

  #for level 0 and 1, since pair-matching algorithm starts iteration at level 2, level 0 and 1 are
  # calculated with predefined functions imported from util.py
  if lvl ==0:
    return zeroType(g,primitive)
  elif lvl ==1:
    oneTypeRes = []
    srcDict = oneTypeDict(g,primitive)
    for value in srcDict.values():
      oneTypeRes.append(value)


    #remove duplicates options are different when primitive types are included
    if rmDuplicates:
      if primitive:
        return removeDuplicates(oneTypeRes)
      else:
        return list(set(oneTypeRes))
    else:
       return oneTypeRes
    
  elif lvl<0:
    print("error, level must not be a negative")

  #for all level higher than 1
  else:
    #we extracts all vertices ids and source and destination pairs of edges  from dataframes
    allVertices = [x["id"] for x in g.vertices.rdd.collect()]
    allEdges= [(x["src"],x["dst"])for x in g.edges.rdd.collect()]
    pair_list = []
    types = []
    VTypeDict = getVTypeDict(g,primitive)
    ETypeDict = getETypeDict(g)

    #creating a pairlist
    for e in allEdges:

      for e2 in allEdges:
        if e2[0]==e[1]:
          pair_list.append([e,e2])
    pairListCopy =pair_list

    #pair-matching
    for h in range(lvl-1):
      for p in pair_list:      
          for cur in pairListCopy:
            if (cur is None) or (p is None): 
              #print("error")
              pass
            else:
              lastE = p[-1]
              firstE = cur[0]
              if lastE == firstE:
                #print("yay")
                pair_list.append(p.append(cur[1]))

    for x in pair_list:
      if x is None:
        #print ("")
        pass
      else:
        if len(x)==lvl:
          types.append(x)

    #creates mapping from pairs to prov types
    provMap = dict((el,[]) for el in allVertices )
    
    for pair in types:
      provMap[pair[0][0]].append(pairToType(pair,VTypeDict,ETypeDict))

    for key,val in provMap.items():
     
      provMap[key] = removeDuplicates(provMap[key])
      if len(provMap[key])>1:
        
        provMap[key]=list(zip(*provMap[key]))
        
    
    allTypes = [x for x in list(provMap.values()) if x != []]
      
    return allTypes

#this function summarizes all the prov types calculated in a feature vector (dictionary) with each type as key and occurences as value
def generateFeatVecPair(g,to_level,primitive):
  rmDuplicates = False
  featVecDict = {}
  for i in range(to_level+1):
    typesList=typesCountPairWise(g,i,primitive,rmDuplicates)
    for fpt in typesList:
      featVecDict[str(fpt)] = typesList.count(fpt)
  return featVecDict

