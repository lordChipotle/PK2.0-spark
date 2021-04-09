import findspark
findspark.init()

import pyspark as spark
number_cores = 8
memory_gb = 24
conf = (
    spark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)
sc = spark.SparkContext(conf=conf)
from functools import reduce

from pyspark.sql.functions import col, lit, when
from graphframes import *
from ..util import*
from .counttypes import*
from ..provtographframe import prov_to_graphframe_motiffind
import pandas as pd
import functools
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.linalg import SparseVector, DenseVector

from prov.model import (
    ProvDocument,
    ProvRecord,
    ProvElement,
    ProvEntity,
    ProvActivity,
    ProvAgent,
    ProvRelation,
    PROV_ATTR_ENTITY,
    PROV_ATTR_ACTIVITY,
    PROV_ATTR_AGENT,
    PROV_ATTR_TRIGGER,
    PROV_ATTR_GENERATED_ENTITY,
    PROV_ATTR_USED_ENTITY,
    PROV_ATTR_DELEGATE,
    PROV_ATTR_RESPONSIBLE,
    PROV_ATTR_SPECIFIC_ENTITY,
    PROV_ATTR_GENERAL_ENTITY,
    PROV_ATTR_ALTERNATE1,
    PROV_ATTR_ALTERNATE2,
    PROV_ATTR_COLLECTION,
    PROV_ATTR_INFORMED,
    PROV_ATTR_INFORMANT,
    PROV_ATTR_BUNDLE,
    PROV_ATTR_PLAN,
    PROV_ATTR_ENDER,
    PROV_ATTR_STARTER,
    ProvBundle,
)
#this function is used to format a file name so it can be searched in a DBFS
#Since DBFS automatically change all "-" and ","to "_". We format it so we can extract relevant label files
#This function will not be used when running on local machine
def formatFileName(f):
  f= f.replace(".json","")
  f=f.replace(".","_")
  f=f.replace("-","_")
  f = f+".json"
  return f

#This function assembles all the feature vectors generated into a spark dataframe directly
def createSparseMtxSparkMotifFinding(filenames,path,lvl,primitive):
    csv_data = path + "/" +"graphs.csv"
    df = pd.read_csv(csv_data)
    #df['graph_file'] = df['graph_file'].apply(lambda x: formatFileName(x))   #uncomment this if running code on Databricks
    csv_dict = dict(zip(list(df.graph_file), list(df.label)))
    
    prov_list = [(ProvDocument.deserialize(path+"/"+f),csv_dict[f])for f in filenames]
    featVecsList = []
    for doc in prov_list:
      featVecs = generateFeatVec(prov_to_graphframe_motiffind(doc[0],primitive),lvl) 
      featVecs["label"] = doc[1]
      featVecsList.append(featVecs)
      
    #inputs the list directly into spark to create a sparse matrix. Not encountered features in a row will be assigned NaN (null value)
    df = sc.parallelize(featVecsList).toDF()
    return df

#we call the matrix assembling function in this function and convert the processed dataframe into temporary orc file for machine learning
#This orc file should be deleted when the program is in production stage
def generateOrcFile(path,lvl,primitive):
  filenames=os.listdir(path)
  jsonfilenames = [f for f in filenames if f.endswith(".json")] #search for json files from the datasets and process those only
  X=createSparseMtxSparkMotifFinding(jsonfilenames,path,lvl,primitive)
  X = X.na.fill(value=0) #replace all the NaNs with 0
  return X
def main():
  mainpath = "/provSpark-datasets"
  dirList = {"/CM-Routes",
              "/CM-RouteSets",
              "/CM-Buildings",
              "/PG-D",
              "/PG-T"
              }
  #generating sparse matrices for each dataset
  for path in dirList:
    X_primitive = generateOrcFile(mainpath+path,2,True)
    X_primitive.write.format("orc").save(mainpath+path+"P"+".orc")
    X_not_primitive = generateOrcFile(mainpath+path,2,False)
    X_not_primitive.write.format("orc").save(mainpath+path+"NP"+".orc")

if __name__ == "__main__":
    main()