import time
from prov.model import ProvDocument, ProvElement, ProvRecord, QualifiedName
from prov.constants import (
    PROV_ENTITY,
    PROV_ACTIVITY,
    PROV_GENERATION,
    PROV_USAGE,
    PROV_COMMUNICATION,
    PROV_START,
    PROV_END,
    PROV_INVALIDATION,
    PROV_DERIVATION,
    PROV_AGENT,
    PROV_ATTRIBUTION,
    PROV_ASSOCIATION,
    PROV_DELEGATION,
    PROV_INFLUENCE,
    PROV_ALTERNATE,
    PROV_SPECIALIZATION,
    PROV_MENTION,
    PROV_MEMBERSHIP,
)
import functools
from pyspark.sql.functions import col, lit, when
from graphframes import *
import pandas as pd
import numpy as np
from pyspark.sql.functions import col
from matplotlib import pyplot as plt


from itertools import chain
from typing import Dict
import requests
from prov.model import ProvDocument

import json
import os
from functools import reduce

from pairmatching.counttypes import typesCountPairWise
from motiffinding.counttypes import functionSelecter
from provtographframe import prov_to_graphframe_motiffind, prov_to_graphframe_pairmatch

def download_prov_json_document(url: str) -> ProvDocument:
    # try to download the provided url
    r = requests.get(url)
    r.raise_for_status()

    # no exception so far, we have successfuly downloaded it
    prov_doc = ProvDocument.deserialize(content=r.text)
    return prov_doc
#simplifying type-counting functions to be called directly with neater format
def M_TypeCount(document,h):
    return functionSelecter(h,prov_to_graphframe_motiffind(document, False))
def P_TypeCount(document,h):
    return typesCountPairWise(prov_to_graphframe_pairmatch(document),h,False,False)
def main():
    # a provenance graph with the size of 13 vertices
    documentSmall = download_prov_json_document("https://openprovenance.org/store/documents/4141.json")
    # a provenance graph with the size of 24 vertices
    documentLarge = download_prov_json_document("https://openprovenance.org/store/documents/282.json")

    document1Mdict = {}
    document2Mdict = {}
    document1Pdict = {}
    document2Pdict = {}

    #run experiments on 3 differe type-counting level: 0, 2,4
    for h in [0, 2, 4]:
        document1list = []
        document2list = []
        #run 5 times and find the average value
        for i in range(5):

            #run on smaller graph
            start = time.perf_counter()
            M_TypeCount(documentSmall,h)
            end = time.perf_counter()
            document1list.append(end - start)
            #run on larger graph
            start1 = time.perf_counter()
            M_TypeCount(documentLarge,h)
            end1 = time.perf_counter()
            document2list.append(end - start)
        document1Mdict[str(h)] = sum(document1list)/len(document1list)
        document2Mdict[str(h)] = sum(document2list)/len(document2list)

        #same process for pair-matching
        document1list = []
        document2list = []
        for i in range(5):
            document1list = []
            document2list = []
            start = time.perf_counter()
            P_TypeCount(documentSmall,h)
            end = time.perf_counter()
            document1list.append(end - start)

            start1 = time.perf_counter()
            P_TypeCount(documentLarge,h)
            end1 = time.perf_counter()
            document2list.append(end - start)
        document1Pdict[str(h)] = sum(document1list)/len(document1list)
        document2Pdict[str(h)] = sum(document2list)/len(document2list)

    dictM = {"Smaller graph":document1Mdict,"Larger graph":document2Mdict}
    dictP = {"Smaller graph":document1Pdict,"Larger graph":document2Pdict}
    df1 = pd.DataFrame({"Smaller graph - Motif":document1Mdict,"Larger graph - Motif":document2Mdict}).transpose()
    df2 = pd.DataFrame({"Smaller graph - Pair":document1Pdict,"Larger graph - Pair":document2Pdict}).transpose()
    pd.set_option('display.width', 40)

    #print out the results of both methods
    print("Motif finding results")
    print(df1)
    print("Pair-matching results")
    print(df2)

    #concat them to be plotted in line chart together
    dfRes = pd.concat([df1,df2]).transpose()

    dfRes.plot.line(figsize=(10,10))
    plt.title("Time performance evaluation")
    plt.xlabel("Level h")
    plt.ylabel("Time")
    plt.savefig('/plots/experiement1.pdf')
    print("experiment 1 plot saved!")
if __name__ == "__main__":
    main()