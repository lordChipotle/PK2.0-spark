from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import pandas as pd
import numpy as np

def getVTypeDict(g):
  idList = [x["id"]for x in g.vertices.rdd.collect()]
  eleTypeList = [x["type"]for x in g.vertices.rdd.collect()]
  return dict(zip(idList,eleTypeList))

def getETypeDict(g):
  edgeList =  [(x["src"],x["dst"])for x in g.edges.rdd.collect()]

  typeList = [x["relation type"]for x in g.edges.rdd.collect()]
  return dict(zip(edgeList,typeList))

def zeroType(g):
  typeList = [x["type"]for x in g.vertices.rdd.collect()]
  types = list(set(typeList))
  return types

def oneType(g):
  relTypeList = [(x["relation type"],x["dst"])for x in g.edges.rdd.collect()]
  VTypeDict = getVTypeDict(g)
  for i in range(len(relTypeList)):
    assocType = relTypeList[i][1]
    relTypeList[i]= (relTypeList[i][0],VTypeDict[assocType])
  return list(set(relTypeList))

def pairToType(listA,Vdict,Edict):
      pairs = listA
      zeroT = Vdict[pairs[-1][-1]]
      for i in range(len(pairs)):
        pairs[i]= Edict[pairs[i]]
      pairs.append(zeroT)
      return pairs

def removeDuplicates(aList):
  temp=[]
  for x in aList:
    if x not in temp:
      temp.append(x)
  return temp
############################unused functions####################################
def filterPotential(g,lvl):
  
  subg = g
  if lvl==1:
    
    return subg.outDegrees
  elif lvl ==0:
     
    return g.vertices
  else:
    for i in range(lvl-1):
      idList = [x["id"]for x in subg.outDegrees.rdd.collect()]
      subg = subg.filterEdges(col("dst").isin(idList))
      subg = subg.filterVertices(col("id").isin(idList))
    display(subg.outDegrees)
    return subg.outDegrees
  
def adjacentDict(g):
  parenthood = {}
  parents = [x["src"]for x in g.edges.rdd.collect()]
  parents = list(set(parents))
  for parent in parents:
    subg = g.filterEdges(col("src")==parent)
    children = [x["dst"]for x in subg.edges.rdd.collect()]
    heritage = (parent,children)
    theList = []
    theList.append(heritage)
    parenthood[parent] = theList
  return parenthood
  


