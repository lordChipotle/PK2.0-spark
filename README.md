# Spark-based Provenance Kernel

A detailed report on this project can be found here: https://drive.google.com/file/d/1rlDwVg5ti06jB2yXWnob2YXnjJXyI_6c/view?usp=sharing
----------------------------------------------------------------------------------------------------------------------
Data collection:

The data we used are published by @trungdong at https://github.com/trungdong/provenance-kernel-evaluation

The directories should be unzipped and placed in the folder provSpark-datasets for the program to run.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Running on the Cloud with Databricks:
* Sign up or sign into a standard Databricks account, configure the AWS workplace environments and storage bucket set up following this tutorial:https://docs.databricks.com/getting-started/account-setup.html
* Set up a cluster with the most recent version of Apache Spark environment.
* Once the cluster is set up, install graphframes packages matching the Apache Spark environment, then the most recent version of MLlib and prov
* Prior to starting, the datasets downloaded earlier from @trungdong should be uploaded to the databricks manually. Here's a link on how to do that: https://docs.databricks.com/data/data.html

* Import notebooks of your selection from the databricks-notebooks folder to the data bricks interface and run through each cell as guided in the notebooks:
	* import **provspark-pair-matching.ipynb** for a demo in pair-matching algorithm
	* import **provspark-motif-finding.ipynb** for a demo in motif-finding algorithm 
	* import **prov-spark-ml** to start the machine learning process and reproduce or generate similar results to experiment 2 and assessment 2 from the report
	* import **performance-comparison** to conduct runtime analysis and generate results similar to experiment 1 and assessment 1.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. Running on local machines:
* Run **pip install -r requirements.txt** to install all packages requirements
* Run **python reproduce_data.py** if the intent is to run machine learning part of the project only (skipping the long waiting of training data generation)
* Navigates to the motiffinding folder using cd command and run **python gen-training-data-motif.py** to generate sparse matrices(training data) with motif finding algorithm
* Likewise, navigates to the pairmatching folder using cd command and run **python gen-training-data-pair.py** to generate sparse matrices(training data) with pair-matching algorithm
* Through our experience of running both on the Cloud, pair-matching is faster
* Navigates back to main directory (/src) and run train.py to start the machine learning process; If the goal is to analyze the results we generated. Skip this step, and copy-paste "final_res.p" from the data-for-reproducing-results folder to the main directory. 
* Run:
	* **python run_experiment1.py** to start runtime analysis between motif find and pair match with different size graphs and levels of type counting)
	
	* **python run_experiment2.py** to generate overall evaluation of the machine learning results
	* **python run_assessment1.py** to start runtime analysis between the original provenance kernel type-counting and 2 type-counting methods we developed (motif find and pair-matching). This file also assesses the runtime between network x package and graphframes package. Benchmarking of both pair-matching and motif finding are included as well
	* **python run_assessment2.py** to generate and plot accuracy scores of the best performing classifiers over all datasets. 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------
We highly recommend to follow the running on the cloud instructions as current compatibility issues have not been resolved between graphframes local release (not limited to the one provided on databricks) and the latest spark's version. One potential way to work around is to downgrade spark's version to the current release of graphframes on PyPI (0.6- Spark version 2.4+ and Scala version 2.11). However, as the majority of the code written in this project including the use of MLlib does not work on old version of Spark, it isn't realistic to recode everything using the older syntax. 



