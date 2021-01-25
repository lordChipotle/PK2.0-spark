from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
from util import*

def typesCount(g,lvl):
  
  if lvl ==0:
    return zeroType(g)
  elif lvl ==1:
    return oneType(g)
  elif lvl<0:
    print("error, level must not be a negative")
  else:
    allVertices = [x["id"] for x in g.vertices.rdd.collect()]
    allEdges= [(x["src"],x["dst"])for x in g.edges.rdd.collect()]
    pair_list = []
    types = []
    VTypeDict = getVTypeDict(g)
    ETypeDict = getETypeDict(g)

    for e in allEdges:

      for e2 in allEdges:
        if e2[0]==e[1]:
          pair_list.append([e,e2])
    pairListCopy =pair_list
    for h in range(lvl):
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
    provMap = dict((el,[]) for el in allVertices )
    
    for pair in types:
      provMap[pair[0][0]].append(pairToType(pair,VTypeDict,ETypeDict))

    for key,val in provMap.items():
     
      provMap[key] = removeDuplicates(provMap[key])
      if len(provMap[key])>1:
        
        provMap[key]=list(zip(*provMap[key]))
        
    
    allTypes = [x for x in list(provMap.values()) if x != []]
      
    return allTypes