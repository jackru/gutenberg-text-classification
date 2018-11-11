# Coursework script - Arthur Jack Russell - Big Data

import sys
import re
from operator import add, itemgetter
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.storagelevel import StorageLevel
import os
from time import time
import shutil
from math import log, ceil, exp


# -------------------------------------------------
# FUNCTION DEFINITIONS AND VARIABLE INITIALISATIONS
# -------------------------------------------------

# Pickle an RDD, overwriting destination if it exists already

def pickle(rdd, destination):
    if os.path.exists(destination):
        shutil.rmtree(destination)
    rdd.saveAsPickleFile(destination)
    

# Transform a term vector into a hash vector of size n, adding contributions per term to
# corresponding hash bin

def f_hfVector(f, wcl, n):
    vec = [0] * n # initialise vector of size n
    for wc in wcl :
        i = hash(wc[0]) % n # hash word and modulo to assign to bins
        vec[i] += wc[1] # add count to index
    return (f, vec)


# --------------------------
# MAIN PROGRAM STARTS HERE
# --------------------------

if __name__ == "__main__":
    startJob = time()
    if len(sys.argv) != 7:
        print >> sys.stderr, "Usage: cwk.py  <host> <root for TF and IDF files>" \
        " <numChunks> <hash bin list> <minimum doc percentage> <destination for TFIDF>"
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
        
        
# Specify the directory where TF and IDF files are stored, how many TF chunks this
# directory contains and the destination directory for hashed TFIDF vectors
        
        root = sys.argv[2][:-1] if sys.argv[2][-1] == "/" else sys.argv[2]
        numChunks = int(sys.argv[3])
        dest = sys.argv[6][:-1] if sys.argv[6][-1] == "/" else sys.argv[6]


# -------------------------------------
# START OF PART 2: CREATE TFIDF VECTORS
# -------------------------------------

        startStage1b = time()
        
        
# A list of hash bin lengths can be passed (e.g. 3000,10000,30000). A full set of hashed
# TFIDF vectors will be created for each length
        
        hb_list = [int(hb) for hb in sys.argv[4].split(',')]
        
        
# Here you can specify whether to truncate the vocabulary, to exclude rare terms.
# Any term that appears in less than this percentage of documents will be excluded.
        
        minDocPerc = float(sys.argv[5])
        
        
# Filter the IDF using the minimum percentage criterion.
        
        IDF = sc.pickleFile('{}/IDF'.format(root)) \
                .filter(lambda (w, idf): exp(-idf) * 100 >= minDocPerc) \
                .cache()
                
        vocabLength = IDF.count()
        print "Truncated vocab length:", vocabLength
        

# Remove existing vectors if space is a constraint (as it was originally with 1GB limit)

#         if os.path.exists('pickles/TFIDF'):
#             shutil.rmtree('pickles/TFIDF')


# --------------------
# CREATE TFIDF VECTORS
# --------------------

        hashedTFIDF = {}
        TFIDF = {}

        for i in range(numChunks):
        

# Skip any missing directories that may have been deleted (when space constraints were
# an issue)

            start = time()        
            if not os.path.exists('{}/TF/TF{:0>3d}'.format(root, i)):
                continue


# Join term-frequency RDD with IDF RDD, multiply TF by IDF per term in flatMap phase,
# then reduce by file ID to create a TFIDF vector per file

            TFIDF[i] = sc.pickleFile('{}/TF/TF{:0>3d}'.format(root, i)) \
                         .join(IDF) \
                         .flatMap(lambda (w, (idTFl, IDF)): \
                          [(id, [(w, TF*IDF)]) for (id, TF) in idTFl]) \
                         .reduceByKey(add, 16) \
                         .cache()
                         

# Hash the TFIDF vectors into fixed length vector(s), as specified by hb_list
                         
            hashedTFIDF[i] = {}
            for hb in hb_list:
                hashedTFIDF[i][hb] = TFIDF[i].map(lambda (id, tflist): \
                                                  f_hfVector(id, tflist, hb))


# Save hash-vectors to disk as pickle files, then un-persist to conserve memory

                pickle(hashedTFIDF[i][hb], '{}/{}/TFIDF{:0>3d}'.format(dest, hb, i))
                hashedTFIDF[i][hb].unpersist()

# Un-persist TFIDF RDD to conserve memory

            TFIDF[i].unpersist()
            
            elapsed = time() - start
            print "Hashed TFIDF vectors saved as pickle files:", '{:.2f}s'.format(elapsed)
            

# Remove existing vectors if space is a constraint (as it was originally with 1GB limit)
        
#             shutil.rmtree('pickles/TF/TF' + '{:0>3d}'.format(i))
#             print "pickles/TF/TF{:0>3d} deleted".format(i)
        
#         shutil.rmtree('pickles/TF')


# Output some interesting information
    
        elapsed = time() - startStage1b
        print "\nTotal elapsed time for Stage 1b:", elapsed, "\n"
    
        totalElapsedTime = time() - startJob
        print "Total time elapsed - just sc.stop() to go:", totalElapsedTime


# End of try-finally block. This ensures that spark context closes cleanly (e.g. deletes
# all its tmp files) in the case of exception or ctrl-C termination.
    
    finally:
        start = time()
        sc.stop()
        elapsed = time() - start
        print "Elapsed stopping SparkContext:", elapsed
        totalElapsedTime = time() - startJob
        print "\nOverall time elapsed:", totalElapsedTime