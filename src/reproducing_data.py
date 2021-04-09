import pandas as pd
from pyspark.sql import DataFrame
import pyspark as spark
number_cores = 8
memory_gb = 24
conf = (
    spark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)
sc = spark.SparkContext(conf=conf)
datasetpath = "/provSpark-datasets"
datasetnames = ["CM-BuildingsNP","CM-BuildingsP","CM-RoutesNP","CM-RoutesP","CM-RouteSetsNP","CM-RouteSetsP","PG-TNP","PG-TP","PG-DNP","PG-DP"]
path = "/data-for-reproducing-results"

def reproduce(path,datasetpath,datasetnames):
    for ds in datasetnames:
        df = spark.read(path+"/"+ds+".csv")
        df.write.format("orc").save(datasetpath+"/"+ds+".orc")
def main():
    reproduce(path,datasetpath,datasetnames)

if __name__ == "__main__":
    main()