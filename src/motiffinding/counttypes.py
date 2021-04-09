import functools
from pyspark.sql.functions import col, lit, when
from graphframes import *
import pandas as pd
import numpy as np
from pyspark.sql.functions import col
from ..util import *


from itertools import chain
from typing import Dict
import requests
from prov.model import ProvDocument

import json
import os


from pyspark.sql import DataFrame

# to count 0-type, we simply extract the prov types column from the vertices dataframe
def typeZero(g):
  typeList = [x["type"]for x in g.vertices.rdd.collect()]
  return typeList

#we define a pattern string for finding 1-types
def typeOne(g):
  motif = g.find("(a)-[e1]->(b);!(b)-[]->(a)")
  edgesList = [[x["e1"],x["a"]]for x in motif.rdd.collect()]
  typeDict = dict((e[1].id,[]) for e in edgesList)
  for e in edgesList:
    zeroType=g.vertices.filter(col("id")==e[0].dst)
    generic = [x["type"]for x in zeroType.rdd.collect()][0]
    fullType = [e[0].relation_type,generic]
    temp = typeDict[e[1].id]
    temp.append(fullType)
    typeDict[e[1].id] = temp
  return typeDict

#we define a pattern string for finding 2-types
def typeTwo(g):
  motif = g.find("(a)-[e1]->(b); (b)-[e2]->(c);!(b)-[]->(a);!(c)-[]->(b)").filter("a.id != c.id")
  edgesList = [[x["e1"],x["e2"],x["a"]]for x in motif.rdd.collect()]
  typeDict = dict((e[2].id,[]) for e in edgesList)
  for e in edgesList:
    zeroType=g.vertices.filter(col("id")==e[1].dst)
    generic = [x["type"]for x in zeroType.rdd.collect()][0]
    fullType = [e[0].relation_type,e[1].relation_type,generic]
    temp = typeDict[e[2].id]
    temp.append(fullType)
    typeDict[e[2].id] = temp
  return typeDict

#defines a pattern string for finding 3-types
def typeThree(g):
  motif = g.find("(a)-[e1]->(b); (b)-[e2]->(c);(c)-[e3]->(d);!(b)-[]->(a);!(c)-[]->(b);!(d)-[]->(c)")
  edgesList = [[x["e1"],x["e2"],x["e3"],x["a"]]for x in motif.rdd.collect()]
  typeDict = dict((e[3].id,[]) for e in edgesList)
  for e in edgesList:
    zeroType=g.vertices.filter(col("id")==e[2].dst)
    generic = [x["type"]for x in zeroType.rdd.collect()][0]
    fullType = [e[0].relation_type,e[1].relation_type,e[2].relation_type,generic]
    temp = typeDict[e[3].id]
    temp.append(fullType)
    typeDict[e[3].id] = temp
  return typeDict
#define a pattern string for finding 4-types
def typeFour(g):
  motif = g.find("(a)-[e1]->(b); (b)-[e2]->(c);(c)-[e3]->(d);(d)-[e4]->(E);!(b)-[]->(a);!(c)-[]->(b);!(d)-[]->(c);!(E)-[]->(d)")
  edgesList = [[x["e1"],x["e2"],x["e3"],x["e4"],x["a"]]for x in motif.rdd.collect()]
  typeDict = dict((e[4].id,[]) for e in edgesList)
  for e in edgesList:
    zeroType=g.vertices.filter(col("id")==e[3].dst)
    generic = [x["type"]for x in zeroType.rdd.collect()][0]
    fullType = [e[0].relation_type,e[1].relation_type,e[2].relation_type,e[3].relation_type,generic]
    temp = typeDict[e[4].id]
    temp.append(fullType)
    typeDict[e[4].id] = temp
  return typeDict
#defines a pattern string for finding 5-types
def typeFive(g):
  motif = g.find("(a)-[e1]->(b); (b)-[e2]->(c);(c)-[e3]->(d);(d)-[e4]->(E);(E)-[e5]->(f);!(b)-[]->(a);!(c)-[]->(b);!(d)-[]->(c);!(E)-[]->(d);!(f)-[]->(E)")
  edgesList = [[x["e1"],x["e2"],x["e3"],x["e4"],x["e5"],x["a"]]for x in motif.rdd.collect()]
  typeDict = dict((e[5].id,[]) for e in edgesList)
  for e in edgesList:
    zeroType=g.vertices.filter(col("id")==e[4].dst)
    generic = [x["type"]for x in zeroType.rdd.collect()][0]
    fullType = [e[0].relation_type,e[1].relation_type,e[2].relation_type,e[3].relation_type,e[4].relation_type,generic]
    temp = typeDict[e[5].id]
    temp.append(fullType)
    typeDict[e[5].id] = temp
  return typeDict


#we use a functionselector to get count types to certain level accordingly
def functionSelecter(i,g):
  if i==0:
    return typeZero(g)
  elif i==1:
    return list(formatter(typeOne(g),1).values())
  elif i==2:
    return list(formatter(typeTwo(g),2).values())
  elif i==3:
    return list(formatter(typeThree(g),3).values())
  elif i==4:
    return list(formatter(typeFour(g),4).values())
  elif i==5:
    return list(formatter(typeFive(g),5).values())
  else:
    print("error")

#this function summarizes all the prov types calculated in a feature vector (dictionary) with each type as key and occurences as value
def generateFeatVec(g,lvl):
  if lvl>5:
    print("cannot be above 5 levels for default training")
  else:
    
    featVecDict = {}
    for i in range(lvl+1):
      typesList = functionSelecter(i,g)
      for fpt in typesList:
        featVecDict[str(fpt)] = typesList.count(fpt)
    return featVecDict
