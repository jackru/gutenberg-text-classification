# Coursework script - Arthur Jack Russell - Big Data

import sys
from pyspark import SparkConf
from pyspark import SparkContext
import os
from time import time
import xml.etree.ElementTree as ET
import shutil
import csv


# -------------------------------------------------
# FUNCTION DEFINITIONS AND VARIABLE INITIALISATIONS
# -------------------------------------------------

# Dict of namespaces allows more easily readable code later on

namespaces = {}
namespaces['subject']     = '{http://purl.org/dc/terms/}subject'
namespaces['description'] = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'
namespaces['taxonomy']    = '{http://purl.org/dc/dcam/}memberOf'
namespaces['value']       = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}value'
namespaces['resource']    = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'
namespaces['ebook']       = '{http://www.gutenberg.org/2009/pgterms/}ebook'
namespaces['about']       = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about'


# Pickle an RDD, overwriting destination if it exists already

def pickle(rdd, destination):
    try:
        shutil.rmtree(destination)
    except OSError:
        pass
    rdd.saveAsPickleFile(destination)


# --------------------------
# MAIN PROGRAM STARTS HERE
# --------------------------

if __name__ == "__main__":
    startJob = time()
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: cwk2.py <meta path>"
        exit(-1)
                      

# Construct a list of file-path strings for all files in meta directory

    filepaths = [(r + '/' + f, f) for r, d, fl in os.walk(sys.argv[1]) for f in fl]
    metaFileCount = len(filepaths)
    print "Total number of metadata files:", metaFileCount
    

# Main code to extract subjects from metadata files

    metaDict = {}
    
# --------------------------------------   
# LOOP OVER FILES, GETTING TREE FOR EACH
# --------------------------------------

# Skip files beginning "." (mainly an issue for local Mac OS)

    for filepath in filepaths:
        if filepath[1][0] == ".":
            continue
        tree = ET.parse(filepath[0])
        root = tree.getroot()
        

# ID is of the form "ebooks/####" - string slice to get numerical part of ID

        ID = root.find(namespaces['ebook']).get(namespaces['about'])[7:]


# Initialise a dict entry as empty list for this file
# Extract name and taxonomy identifier for each subject and append to the list

        metaDict[ID] = []
        for subject in root.iter(namespaces['subject']):
            for desc in subject.findall(namespaces['description']):
                value = desc.find(namespaces['value']).text
                taxonomy = desc.find(namespaces['taxonomy']).get(namespaces['resource'])
                metaDict[ID].append((value.encode('utf-8'), taxonomy.encode('utf-8')))

# --------------------------------------
# END OF LOOP OVER FILES
# --------------------------------------
                

# This code outputs the results for monitoring

#     with open('meta.txt', 'w') as f1:
#         f1.write(str(metaDict))
#         
#     idSubjects = [(str(ID), subject[0], subject[1]) \
#               for ID, subjects in metaDict.iteritems() for subject in subjects]
#     with open('meta.csv', 'wb') as f2:
#         writer = csv.writer(f2)
#         writer.writerows(idSubjects)


# ------------------------------------------
# Parallelise and pickle meta dict as an RDD
# ------------------------------------------

# Same config can be used for both local and lewes as resource usage is very light
    
    config = SparkConf().set("spark.executor.memory", "4G") \
                        .set("spark.ui.port", "2410") \
                        .setMaster("local[2]")

    try:
        sc = SparkContext(conf=config, appName="acka630")
        meta = sc.parallelize(metaDict.items(), 16)
        pickle(meta, 'pickles/meta')
    finally:
        sc.stop()
        totalTime = time() - startJob
        print "Total time elapsed:", totalTime
