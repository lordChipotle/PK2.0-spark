import os
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest, NaiveBayes, MultilayerPerceptronClassifier,RandomForestClassifier,DecisionTreeClassifier,LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pyspark as spark
import pandas as pd
number_cores = 8
memory_gb = 24
conf = (
    spark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)
sc = spark.SparkContext(conf=conf)

try:
    import cPickle as pickle
except ImportError:  # Python 3.x
    import pickle

#this function is used to determine the size of classes(types of labels)
def gen_labelDict(path):
  data = spark.read.orc(path)
  labelList = data.select('label').distinct().rdd.map(lambda r: r[0]).collect()
  labelDict = {}
  for i in range(len(labelList)):
    labelDict[labelList[i]] = i
  return labelDict

#this function vectorize columns of data into feature vectors (actual mathematical vectors)
def formatData(path):
  data = spark.read.orc(path)
  labelDict = gen_labelDict(path)
  data = data.toPandas().replace({"label": labelDict})
  spark.conf.set("spark.sql.execution.arrow.enabled", "true")
  data = spark.createDataFrame(data)
  assembler = VectorAssembler(
    inputCols=[c for c in data.columns if c != 'label'],
    outputCol="features")
  df = assembler.transform(data).select("features", "label")
  return df

#crossvalidation function
def evaluate(classifier, train, test, selectedEvaluator):
  paramGrid = ParamGridBuilder().build()
  #10-fold cross-validation
  crossval = CrossValidator(estimator=classifier,
                            evaluator=selectedEvaluator,
                            estimatorParamMaps=paramGrid,
                            numFolds=10)
  model = crossval.fit(train)

  model_metrics = model.avgMetrics #average cross validation score on training data
  transformed_data = model.transform(test)
  test_metrics = selectedEvaluator.evaluate(transformed_data) #cross validation score from test data
  return (model_metrics, test_metrics)


def trainClassifiers(mainpath, dataset):
  testpath = mainpath + "/" + dataset
  labelDict = gen_labelDict(testpath)
  df = formatData(testpath)
  # both label indexer and feature index are necessary for tree classifiers such as DT and RF
  labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)

  featureIndexer = \
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df)

  # generate the train/test split.
  (train, test) = df.randomSplit([0.7, 0.3])
  # instantiate the base classifier.
  lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)
  classifierDict = {
    "LR": lr,
    "SVM": LinearSVC(maxIter=10, regParam=0.1),
    "OvR": OneVsRest(classifier=lr),
    "NB": NaiveBayes(smoothing=1.0, modelType="multinomial"),
    "RF": RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10),
    "DT": DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
  }
  if len(labelDict) > 2:
    classifierDict.pop("SVM") #MLlib only supports linear SVM. therefore SVM can't be used for multiclass training
  accDict = {}

  for classifier in classifierDict.keys():
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, classifierDict[classifier]])

    if (classifier == "DT") or (classifier == "RF"):
      #if the iterated classifier is Decision tree or Random forest, we assigned their evaluation columns properly
      # (this is where we debugged experiment 2 in the report)
      evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction",
                                                    metricName="accuracy")
      accDict[classifier] = evaluate(pipeline, train, test, evaluator)
    else:
      evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
      accDict[classifier] = evaluate(pipeline, train, test, evaluator)

  return accDict

#function for outputing average cross-validation score only
def queryCrossvalAvg(result):
  queriedResult = {}
  for res in result.keys():
    queriedResult[res] = result[res][0][0]
  return {"Accuracy":queriedResult}

#function for outputing cross-validation score on test data only
def queryCrossvalTest(result):
  queriedResult = {}
  for res in result.keys():
    queriedResult[res] = result[res][1]
  return {"Accuracy":queriedResult}



#conduct ml training on all datasets
def produceOverallResults(mainpath,datasetsNP,datasetsP):
  NP_dict = {}
  P_dict = {}
  #train data on PK-NP (prov kernel not including primitive types) data
  for dataset in datasetsNP:
    NP_dict[dataset]=queryCrossvalTest(trainClassifiers(mainpath,dataset))

  #train data on PK-P (prov kernel including primitive types) data
  for datasetP in datasetsP:
    P_dict[datasetP]=queryCrossvalTest(trainClassifiers(mainpath,datasetP))
  return [NP_dict,P_dict]

def main():
  mainpath = "/provSpark-datasets"
  datasets_dir = [ name for name in os.listdir(mainpath) if os.path.isdir(os.path.join(mainpath, name)) ]
  datasets_NP = [name + "NP.orc" for name in datasets_dir]
  datasets_P =  [name + "P.orc" for name in datasets_dir]
  ovr_res = produceOverallResults(mainpath,datasets_NP,datasets_P)

  #store the result in current directory as "final_res.p" for later analysis on accuracy and performance
  with open(mainpath + '/' + 'final_res.p', 'wb') as fp:
    pickle.dump(ovr_res, fp, protocol=pickle.HIGHEST_PROTOCOL)

  #overwrite and clean the data we type-counted
  column_names = ["data", "is", "deleted"]
  df = pd.DataFrame(columns=column_names)
  dfEmpty = spark.createDataFrame(df.astype(str))
  for name in datasets_NP:
    dfEmpty.write.format("orc").save(mainpath+"/"+name)
  for name in datasets_P:
    dfEmpty.write.format("orc").save(mainpath+"/"+name)


if __name__ == "__main__":
    main()
