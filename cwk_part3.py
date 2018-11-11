# Coursework script - Arthur Jack Russell - Big Data

import sys
from operator import add, itemgetter
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.tree import DecisionTree
import os
from time import time
import pandas as pd
import numpy as np


# -------------------------------------------------
# FUNCTION DEFINITIONS AND VARIABLE INITIALISATIONS
# -------------------------------------------------

# Helper function for results dict - returns 0 if no entry is found for a given quadrant
# of the confusion matrix (as opposed to error)

def ifna(key, dic):
    try:
        return dic[key]
    except (KeyError):
        return 0
        

# Zero division error handling to allow F1 scores to be calculated

def iferror(numerator, denominator, error_return_value):
    try:
        return numerator/denominator
    except (ZeroDivisionError):
        return error_return_value
        

# This function calculates key output metrics and saves them as an entry in the supplied
# resultsDict, to save triplicating code (once for each modelling function).
# WARNING: makes reference to global variables

def rMetrics(method, regParam, Set, results, resultsDict, ET, EC):
    truePos = ifna((1, 1.0), results)
    falsePos = ifna((0, 1.0), results)
    trueNeg = ifna((0, 0.0), results)
    falseNeg = ifna((1, 0.0), results)
    total = truePos + falsePos + trueNeg + falseNeg
    accuracy = float(truePos + trueNeg) / total
    precision = iferror(float(truePos), truePos + falsePos, 0.0)
    recall = iferror(float(truePos), truePos + falseNeg, 0.0)
    f1score = iferror(2 * precision * recall, precision + recall, 0.0)
    
    resultMetrics = {}
    resultMetrics['trainTime'] = ET
    resultMetrics['classifyTime'] = EC
    resultMetrics['accuracy'] = accuracy
    resultMetrics['precision'] = precision
    resultMetrics['recall'] = recall
    resultMetrics['truePos'] = truePos
    resultMetrics['falsePos'] = falsePos
    resultMetrics['trueNeg'] = trueNeg
    resultMetrics['falseNeg'] = falseNeg
    resultMetrics['f1score'] = f1score
    
    resultsDict[(subject, method, regParam, j, Set)] = resultMetrics
    
    
# RUN A NAIVE BAYES MODEL

# Training
    
def nBayes(resultsDict, Lambda=1.0):
    start = time()
    nbModel = NaiveBayes.train(trainSetLP[j], Lambda)
    ET = time() - start

# Classify all sets (validation, training and test) using the model, and pass results
# to the rMetrics function so they are added to results summary dict
    
    startClassify = time()
    
    start = time()
    validPredict = validSet[j].map(lambda (lbl, vec): ((lbl, nbModel.predict(vec)), 1))
    validResults = validPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("NBay", Lambda, "Validation", validResults, resultsDict, ET, EC)
    
    start = time()
    trainPredict = trainSet[j].map(lambda (lbl, vec): ((lbl, nbModel.predict(vec)), 1))
    trainResults = trainPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("NBay", Lambda, "Training", trainResults, resultsDict, ET, EC)
    
    start = time()
    testPredict = testSet.map(lambda (lbl, vec): ((lbl, nbModel.predict(vec)), 1))
    testResults = testPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("NBay", Lambda, "Test", testResults, resultsDict, ET, EC)

    print "; Training:", '{:.2f}s'.format(ET), "; Classification:", \
            '{:.2f}s'.format(time() - startClassify)
      

# RUN A DECISION TREE MODEL

# Training

def dTree(resultsDict, MaxDepth=3, MaxBins=10):
    start = time()
    catFeaturesInfo = {}
    trModel = DecisionTree.trainClassifier(trainSetLP[j], numClasses = 2, \
    categoricalFeaturesInfo=catFeaturesInfo, maxDepth = MaxDepth, maxBins = MaxBins)
    ET = time() - start

# Classify all sets (validation, training and test) using the model, and pass results
# to the rMetrics function so they are added to results summary dict

    startClassify = time()
    
    start = time()
    validPredict = trModel.predict(validSet[j].map(lambda x: x[1])).map(lambda x: int(x))
    validResults = trueLabels[j]['valid'].zip(validPredict).countByValue()
    EC = time() - start
    rMetrics("Tree", (MaxDepth, MaxBins), "Validation", validResults, resultsDict, ET, EC)
    
    start = time()
    trainPredict = trModel.predict(trainSet[j].map(lambda x: x[1])).map(lambda x: int(x))
    trainResults = trueLabels[j]['train'].zip(trainPredict).countByValue()
    EC = time() - start
    rMetrics("Tree", (MaxDepth, MaxBins), "Training", trainResults, resultsDict, ET, EC)
    
    start = time()
    testPredict = trModel.predict(testSet.map(lambda x: x[1])).map(lambda x: int(x))
    testResults = trueLabels['test'].zip(testPredict).countByValue()
    EC = time() - start
    rMetrics("Tree", (MaxDepth, MaxBins), "Test", testResults, resultsDict, ET, EC)

    print "; Training:", '{:.2f}s'.format(ET), "; Classification:", \
            '{:.2f}s'.format(time() - startClassify)
    
    
# RUN A LOGISTIC REGRESSION MODEL

# Training
    
def logR(resultsDict, RegType=None, RegParam=1.0):
    start = time()
    lrModel = LogisticRegressionWithSGD.train(trainSetLP[j], iterations=100, \
                step=1.0, miniBatchFraction=1.0, initialWeights=None, \
                regParam=RegParam, regType=RegType, intercept=False)
    ET = time() - start

# Classify all sets (validation, training and test) using the model, and pass results
# to the rMetrics function so they are added to results summary dict

    startClassify = time()
    
    start = time()
    validPredict = validSet[j].map(lambda (lbl, vec): ((lbl, lrModel.predict(vec)), 1))
    validResults = validPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("LogR", (RegType, RegParam), "Validation", validResults, resultsDict, ET, EC)
    
    start = time()
    trainPredict = trainSet[j].map(lambda (lbl, vec): ((lbl, lrModel.predict(vec)), 1))
    trainResults = trainPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("LogR", (RegType, RegParam), "Training", trainResults, resultsDict, ET, EC)
    
    start = time()
    testPredict = testSet.map(lambda (lbl, vec): ((lbl, lrModel.predict(vec)), 1))
    testResults = testPredict.reduceByKey(add).collectAsMap()
    EC = time() - start
    rMetrics("LogR", (RegType, RegParam), "Test", testResults, resultsDict, ET, EC)

    print "; Training:", '{:.2f}s'.format(ET), "; Classification:", \
            '{:.2f}s'.format(time() - startClassify)


# This output function writes a nicely formatted csv that is easy to pivot and analyse
# in Excel (using the pandas.DataFrame.to_csv method)

def output(resultSummary, destination):
    resultsDataframe = pd.DataFrame(resultSummary).T
    resultsDataframe.to_csv(destination)
 

# --------------------------
# MAIN PROGRAM STARTS HERE
# --------------------------

if __name__ == "__main__":
    startJob = time()
    if len(sys.argv) != 7:
        print >> sys.stderr, "Usage: cwk3.py <host> <TFIDF parent folder> " \
        "<meta folder> <rank list> <numFolds> <output.csv>"
        exit(-1)

    host = sys.argv[1]


# This logic enables running the same script with two different Spark Configs.
# On Lewes, a custom UI port is configured to allow monitoring, and tmp file creation
# is sent to a user directory to decouple performance and allow monitoring.

    if host == "lewes":
        config = SparkConf().set("spark.executor.memory", "8G") \
                            .set("spark.ui.port", "2410") \
                            .setMaster("local[2]") \
                            .set("spark.local.dir", "/data/student/acka630/tmp")
    elif host == "local":
        config = SparkConf().set("spark.executor.memory", "4G") \
                            .set("spark.ui.port", "2410")
    else:
        print "Please enter a known host: 'local' or 'lewes'"
        exit(-1)


# The code from here on is wrapped in a try-finally block to ensure sc.stop() cleans up in
# the event of an exception or a ctrl-C termination

    try:
        sc = SparkContext(conf=config, appName="acka630")
    
        vecdir = sys.argv[2] if sys.argv[2][-1] == "/" else sys.argv[2] + "/"
        metadir = sys.argv[3]


# Load TFIDF vectors from the specified directory, and cache as these will be used
# each time a new subject/fold is run

        tfidfVectorsAll = sc.parallelize([], 16)
        for tfidf in os.listdir(vecdir):
            if tfidf[:5] != 'TFIDF': continue
            vectors = sc.pickleFile(vecdir + tfidf)
            tfidfVectorsAll = tfidfVectorsAll.union(vectors)
    
        numVectors = tfidfVectorsAll.cache().count()
    
        print "numVectors:", numVectors


# Generate a list of all file IDs and collect to Python

        fileIdList = tfidfVectorsAll.keys().collect()


# Filter the metadata by file ID to just keep relevant file-subject pairs

        metaData = sc.pickleFile(metadir) \
                     .filter(lambda x: int(x[0]) in fileIdList)
                     

# Reverse the key-value ordering to enable counting of top-N subjects
                 
        topicIdList = metaData.flatMap(lambda (ID, topics): \
                              [(topic, ID) for (topic, taxonomy) in topics])

        countsBySubject = sorted(list(topicIdList.countByKey().items()), \
                                key=itemgetter(1), reverse=True)


# Alternative code allows fromRank and toRank to be specified (need to change sys.args)

#         fromRank = int(sys.argv[4])
#         toRank = int(sys.argv[5])
#        print "Top subjects:", countsBySubject[fromRank-1:toRank]
#         topN = [i[0] for i in countsBySubject[fromRank-1:toRank]]


# This code parses the comma-separated rank list to generate a list of subjects

        rankList = [int(rank) for rank in sys.argv[4].split(',')]
        print "Top subjects:", [countsBySubject[rank-1] for rank in rankList]
        topN = [countsBySubject[rank-1][0] for rank in rankList]


# Read sys.args specifying number of folds & results destination. Initialise results dict
    
        numFolds = int(sys.argv[5])
        outFile = sys.argv[6]
        resultSummary = {}
    

# ----------------------------------------
# START OF MODELLING CODE FOR EACH SUBJECT
# ----------------------------------------

        for subject in topN:

# Generate a list of file IDs per subject. This is OK as there are no duplicates.
# Use countByValue().keys().collect() if there are duplicates

            subjectFileIDs = topicIdList.filter(lambda x: x[0] == subject) \
                                        .map(lambda (k, v): int(v)) \
                                        .collect()


# Use set logic to enable i) easy de-duping; and ii) subtraction of positive set of
# file IDs from total set to give negative set

            posSet = set(subjectFileIDs)
            negSet = set(fileIdList) - posSet
            numPos = len(posSet)
            numNeg = len(negSet)
        
            print "Subject:", subject, "; numPos:", numPos


# Partition into stratified folds by sampling a 1/numFolds fraction each of positive and
# negative file IDs and removing in turn from the candidate set for the next fold

            foldFiles = {}
            for i in range(numFolds):
                posSample = np.random.choice(list(posSet), numPos/numFolds, False)
                negSample = np.random.choice(list(negSet), numNeg/numFolds, False)
                posSet -= set(posSample)
                negSet -= set(negSample)
                
                
# Combine sets and add labels to complete construction of the fold
                
                foldFiles[i] = [(f, 1) for f in posSample] + [(f, 0) for f in negSample]
        

# Create file list RDDs using sc.parallelize()
# Materialise fold RDDs by joining with these & cache as they will be used multiple times

            foldRDD = {}
            for i in range(numFolds):
                foldRDD[i] = sc.parallelize(foldFiles[i], 16) \
                               .join(tfidfVectorsAll, 16) \
                               .map(lambda (f, (lbl, vec)): (lbl, vec)) \
                               .cache()


# Take out test fold now. COUnt and  cache true labels so dTree doesn't need to calculate
# them each time      

            testSet = foldRDD[numFolds-1].cache()
            trueLabels = {}
            trueLabels['test'] = testSet.map(lambda x: x[0]).cache()
            start = time()
            print "Constructing test set:", trueLabels['test'].countByValue().items(), \
                    "; time elapsed:", '{:.2f}s'.format(time() - start)
            
            
# Initialise dicts for training and validation sets
            
            trainSet = {}
            trainSetLP = {}
            validSet = {}


# ------------------------------------------------
# START OF LOOP OVER TRAINING AND VALIDATION FOLDS
# ------------------------------------------------

            for j in range(numFolds-1):
                print "Commencing fold", j
                
# Validation set is denoted by j
                
                validSet[j] = foldRDD[j].cache()
                
                
# Training set is the remainder of folds. Use set subtraction to take fold j away

                trainSet[j] = sc.parallelize([], 16)
                for k in set(range(numFolds-1)) - {j}:
                    trainSet[j] = trainSet[j].union(foldRDD[k])
                trainSet[j].cache()
                               
                
# Create labeled point for Naive Bayes and Logistic Regression models, and true label
# RDDs for Decision Tree model
                

                trueLabels[j] = {}
                trueLabels[j]['train'] = trainSet[j].map(lambda x: x[0]).cache()
                trueLabels[j]['valid'] = validSet[j].map(lambda x: x[0]).cache()
                trainSetLP[j] = trainSet[j].map(lambda (lbl, v): LabeledPoint(lbl, v)) \
                                           .cache()

                
                
# Performance is unaffected but easier to monitor if I force materialisation here by
# counting, plus enables printing of some interesting info

                start = time()
                print "Constructing training set:", \
                        trueLabels[j]['train'].countByValue().items(), \
                        "; time elapsed:", '{:.2f}s'.format(time() - start)
                start = time()
                print "Constructing validation set:", \
                        trueLabels[j]['valid'].countByValue().items(), \
                        "; time elapsed:", '{:.2f}s'.format(time() - start)
                start = time()
                print "Constructing labelled point training vectors:", \
                        trainSetLP[j].count(), "; time elapsed:", \
                        '{:.2f}s'.format(time() - start)



# ----------
# RUN MODELS
# ----------

# For each model, loop over the required RegParam settings, run the model, and output
# results to csv

                print "Naive Bayes"
                for Lambda in [0.5, 0.1]:
                    print "Lambda:", Lambda,
                    nBayes(resultSummary, Lambda)
                    output(resultSummary, outFile)
                
                print "Decision Trees"
                for MaxDepth, MaxBins in [(3, 5), (3, 10), (4, 5)]:
                    print "MaxD, MaxB:", MaxDepth, MaxBins,
                    dTree(resultSummary, MaxDepth, MaxBins)
                    output(resultSummary, outFile)
                
                print "Logistic Regression"
                print "RegType, RegParam: None, 0.0",
                logR(resultSummary, None, 0.0)
                output(resultSummary, outFile)
#                 for RegType in ["l1", "l2"]:
#                     for RegParam in [0.1, 0.001]:
#                         print "RegType, RegParam:", RegType, RegParam,
#                         logR(resultSummary, RegType, RegParam)
#                         output(resultSummary, outFile)


# Un-persist RDDs specific to this fold to conserve memory

                validSet[j].unpersist()
                trainSet[j].unpersist()
                trainSetLP[j].unpersist()
                trueLabels[j]['train'].unpersist()
                trueLabels[j]['valid'].unpersist()


# ------------------------------------------------
# END OF LOOP OVER TRAINING AND VALIDATION FOLDS
# ------------------------------------------------


# Un-persist RDDs specific to this subject to conserve memory

            testSet.unpersist()
            trueLabels['test'].unpersist()
            for i in range(numFolds):
                foldRDD[i].unpersist()
                
                
# -------------------------
# END OF LOOP OVER SUBJECTS
# -------------------------


    finally:
        sc.stop()
