import pandas as pd
import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt

#function for plotting results from a specific dataset
def plotRes(res,dataset_name,shouldTranspose):
  dfRes = pd.DataFrame(res)
  #transpose the dataframes if wants to pivot the columns and rows
  if shouldTranspose:
    dfRes = dfRes.transpose()
  dfRes.plot(kind="bar", title="test")
  plt.title("Evaluation of "+ dataset_name)
  plt.xlabel("Models")
  plt.ylabel("Values")

#search for best-performing classifier
def findBestClassifier(res):
  acc = res['Accuracy']
  classifier = max(acc, key=acc.get)
  return (classifier,acc[classifier])

#find best-performing classifiers in each dataset (both primitive or non-primitive)
def P_NP_Comparison(resNP, resP, datasets, datasetsP):
    NP_dict = {}
    P_dict = {}
    for dataset in datasets:
        NP_dict[dataset] = findBestClassifier(resNP[dataset])[1]
    for datasetP in datasetsP:
        P_dict[datasetP] = findBestClassifier(resP[datasetP])[1]
    return {"PK-NP": NP_dict, "PK-P": P_dict}

#designed to merge results from both PK-NP anf PK-P
def organized(dict1,dict2):
  fullOrganizedDict={}
  list1 = list(dict1)
  list2 = list(dict2)
  for i in range(len(list1)):

    fullOrganizedDict[list1[i]]= dict1[list1[i]]
    fullOrganizedDict[list2[i]] = dict2[list2[i]]
  return fullOrganizedDict

#used for plotting the final graph: accuracy results of best performing classifiers of all datasets
def splot(dictsaved, datasets, datasetsP):
    sns.set(rc={'figure.figsize': (20, 8.27)})
    #put both PK-NP and PK-P's results in a dataframe
    df = pd.DataFrame(organized(P_NP_Comparison(dictsaved[0], dictsaved[1], datasets, datasetsP)["PK-NP"],
                                P_NP_Comparison(dictsaved[0], dictsaved[1], datasets, datasetsP)["PK-P"]).items())

    splot = sns.barplot(x=0, y=1, data=df.iloc[0:10])
    for p in splot.patches:
        splot.annotate(str(p.get_height().round(4)), ((p.get_x() * 1.05), (p.get_height() * 1.05)))

    xlabelstr = "Datasets"

    splot.set_xlabel(xlabelstr, size=14)
    splot.set_ylabel("Best Classifier's Accuracy", size=14)
    #save the plot to plots folder
    splot.figure.savefig( "plots/performance.pdf")

#find the best classifier of each dataset and extract their values
def bestClassifierDict(dictsaved,datasets,datasetsP,primitive):
  bestCdict = {}
  if primitive:
    dtsd = dictsaved[1]
    datasrc = datasetsP
  else:
    dtsd = dictsaved[0]
    datasrc = datasets
  for data in datasrc:
    bestCdict[data]=(findBestClassifier(dtsd[data]))
  return bestCdict