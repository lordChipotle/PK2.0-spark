from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import pandas as pd
import numpy as np
######################## Pair-matching functions ###############################

#We use this function to find the prov types of vertices
def getVTypeDict(g,primitive):
  idList = [x["id"]for x in g.vertices.rdd.collect()]
  if primitive:
    eleTypeList = [(x["type"],x["primitive type"])for x in g.vertices.rdd.collect()]
  else:
    eleTypeList = [x["type"]for x in g.vertices.rdd.collect()]
  return dict(zip(idList,eleTypeList))

#same function as above but for edges
def getETypeDict(g):
  edgeList =  [(x["src"],x["dst"])for x in g.edges.rdd.collect()]

  typeList = [x["relation type"]for x in g.edges.rdd.collect()]
  return dict(zip(edgeList,typeList))

#this function simply returns the prov types of all vertices which are essentially all the 0-types in this graph
def zeroType(g,primitive):
  if primitive:
    typeList = [(x["type"],x["primitive type"])for x in g.vertices.rdd.collect()]
  else:
    typeList = [x["type"]for x in g.vertices.rdd.collect()]

  return typeList

#This function finds all the edges of 1 level that origin from a vertice. Then iterate the entire graph to find all the 1-types
def oneTypeDict(g,primitive):
  relTypeList = [(x["relation type"],x["src"],x["dst"])for x in g.edges.rdd.collect()]
  VTypeDict = getVTypeDict(g,primitive)
  srcDict = {}
  for i in range(len(relTypeList)):
    if relTypeList[i][1] not in srcDict.keys():
      temp = []
      temp.append([relTypeList[i][0],VTypeDict[relTypeList[i][2]]])
      srcDict[relTypeList[i][1]] = temp
    else:
      temp = srcDict.get(relTypeList[i][1])
      temp.append([relTypeList[i][0],VTypeDict[relTypeList[i][2]]])
      srcDict[relTypeList[i][1]] = temp
  return srcDict

#this function "opens" a pairs and convert the elemnt within to prov types
def pairToType(listA,Vdict,Edict):
      pairs = listA
      zeroT = Vdict[pairs[-1][-1]]
      for i in range(len(pairs)):
        pairs[i]= Edict[pairs[i]]
      pairs.append(zeroT)
      return pairs

#this function is used to remove duplicates for certain functions
def removeDuplicates(aList):
  temp=[]
  for x in aList:
    if x not in temp:
      temp.append(x)
  return temp


############################Motif-finding functions####################################

#this is a function to simply join all encountered prov type to a dictionary with their starting vertex as key, then accordingly format into prov types
def formatter(typeDict,lvl):
  for key,value in typeDict.items():
    fullType = []
    for i in range(lvl+1):
      variety = []
      for v in value:
        if v[i] not in variety:
          variety.append(v[i])
      
      fullType.append("[" + "|".join(variety) + "]")
    typeDict[key] = "→".join(fullType)
  return typeDict

