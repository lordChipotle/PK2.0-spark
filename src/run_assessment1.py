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
from functools import partial
from collections import Counter, defaultdict
from typing import Dict, Iterable, FrozenSet, Set, Tuple

from itertools import chain
from typing import Dict
import requests
from prov.model import ProvDocument

import json
import os
from functools import reduce

from pairmatching.counttypes import typesCountPairWise, generateFeatVecPair
from motiffinding.counttypes import functionSelecter, generateFeatVec
from provtographframe import prov_to_graphframe_motiffind, prov_to_graphframe_pairmatch
from prov.graph import prov_to_graph
from referencedCode import PKv1
def download_prov_json_document(url: str) -> ProvDocument:
    # try to download the provided url
    r = requests.get(url)
    r.raise_for_status()

    # no exception so far, we have successfuly downloaded it
    prov_doc = ProvDocument.deserialize(content=r.text)
    return prov_doc


#simplifying the major functions for both motiffinding and pair-matching
def M_TypeCount(document,h):
    return functionSelecter(h,prov_to_graphframe_motiffind(document, False))
def P_TypeCount(document,h):
    return typesCountPairWise(prov_to_graphframe_pairmatch(document),h,False,False)
def M_genFV(document,h):
    return generateFeatVec(prov_to_graphframe_motiffind(document, False),h)
def P_genFV(document,h):
    return generateFeatVecPair(prov_to_graphframe_pairmatch(document),h,False)

#run runtime comparison of converting and loading graph between networkx and graphframes
def run_nx_vs_gf(document):
    start = time.perf_counter()
    g = prov_to_graph(document)
    end = time.perf_counter()
    nx_Time = end - start

    start = time.perf_counter()
    g = prov_to_graphframe_pairmatch(document)
    end = time.perf_counter()
    gf_Time = end - start
    plt.ylabel("Time relative to Network X Conversion function")
    plt.bar(["Network X conversion time", "Graphframes conversion time"], [nx_Time / nx_Time, gf_Time / nx_Time])
    plt.savefig('nx_vs_gf.png', dpi=400)
    plt.show()
    print("NetworkX vs Graphframes plot generated!")
def benchmarkingM(document):
    fv = [] #feature vector generation runtime results
    tcp = [] #type counting processes runtime results
    tc = [] #type counting(finding a motif) runtime results
    # loop 5 times and find average value
    for i in range(5):
        start = time.perf_counter()
        M_genFV(document,2)
        end = time.perf_counter()
        fv.append(end - start)

        start = time.perf_counter()
        M_TypeCount(document, 2)
        end = time.perf_counter()
        tcp.append(end - start)

        start = time.perf_counter()
        g = prov_to_graphframe_motiffind(document,False)
        g.find("(a)-[e1]->(b); (b)-[e2]->(c);!(b)-[]->(a);!(c)-[]->(b)").filter("a.id != c.id")
        end = time.perf_counter()
        tc.append(end - start)
    fvtime = sum(fv) / len(fv)
    tcptime = sum(tcp) / len(tcp)
    tctime = sum(tc) / len(tc)
    plt.bar(["feature vector generation","type-counting", "motif-find"],
            [fvtime / tctime, tcptime / tctime, tctime / tctime])
    plt.savefig('/plots/benchmarkM.pdf')
    print("plot of benchmarking of motif finding saved!")
def benchmarkingP(document):
    fv = [] #feature vector generation runtime results
    tcp = [] #type counting processes runtime results
    tc = [] #type counting(on pre-generated graph) runtime results
    # loop 5 times and find average value
    for i in range(5):
        start = time.perf_counter()
        P_genFV(document, 2)
        end = time.perf_counter()
        fv.append(end - start)

        start = time.perf_counter()
        P_TypeCount(document, 2)
        end = time.perf_counter()
        tcp.append(end - start)

        g = prov_to_graphframe_pairmatch(document)
        start = time.perf_counter()
        typesCountPairWise(g, 2, False, False)
        end = time.perf_counter()
        tc.append(end - start)
    fvtime = sum(fv) / len(fv)
    tcptime = sum(tcp) / len(tcp)
    tctime = sum(tc) / len(tc)
    plt.bar(["feature vector generation","type-counting", "pre-converted type-counting"],
            [fvtime / tctime, tcptime / tctime, tctime / tctime])
    plt.savefig('/plots/benchmarkP.pdf')
    print("plot of benchmarking of pair-matching saved!")
#run runtime comparison among pk 1.0, pk 2.0 motiffind, and pk 2.0 pair-matching
def run_pk_comparisons(document):
    PK1res = []
    PK2Mres =[]
    PK2Pres = []
    #loop 5 times and find average value
    for i in range(5):
        start = time.perf_counter()
        PKv1(document,2)
        end = time.perf_counter()
        PK1res.append(end - start)

        start = time.perf_counter()
        M_TypeCount(document,2)
        end = time.perf_counter()
        PK2Mres.append(end - start)

        start = time.perf_counter()
        P_TypeCount(document,2)
        end = time.perf_counter()
        PK2Pres.append(end - start)
    pk1time = sum(PK1res)/len(PK1res)
    pk2mtime = sum(PK2Mres)/len(PK2Mres)
    pk2ptime = sum(PK2Pres)/len(PK2Pres)
    plt.bar(["PKv1","PKv2 Motif-finding", "PKv2 Pair-matching"], [pk1time/pk1time,pk2mtime/pk1time, pk2ptime/pk1time])
    plt.savefig('/plots/pks_comparison.pdf')
    print("plot of PKs comparison saved!")
def main():
    document = download_prov_json_document("https://openprovenance.org/store/documents/282.json")
    run_pk_comparisons(document)
    benchmarkingM(document)
    benchmarkingP(document)
    run_nx_vs_gf(document)

if __name__ == "__main__":
    main()