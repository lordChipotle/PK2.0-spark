try:
    import cPickle as pickle
except ImportError:  # Python 3.x
    import pickle
import os
import pandas as pd
import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt
from assessment_functions import *
def main():
    #load the results file from machine learning classification
    mainpath = "/provSpark-datasets"
    with open(mainpath+'/'+'final_res.p', 'rb') as fp:
        dictsaved = pickle.load(fp)
    datasets_dir = os.listdir(mainpath)
    datasets_NP = [name + "NP.orc" for name in datasets_dir]
    datasets_P =  [name + "P.orc" for name in datasets_dir]

    #plot accuracy score of all highest-performing classifiers in all datasets
    splot(dictsaved,datasets_NP,datasets_P)

    #print best classifiers from both pk-np and pk-p
    print("Best classifiers in PK-NPs are: ")
    print(bestClassifierDict(dictsaved,datasets_NP,datasets_P,False))
    print("Best classifiers in PK-Ps are: ")
    print(bestClassifierDict(dictsaved,datasets_NP,datasets_P,True))

if __name__ == "__main__":
    main()