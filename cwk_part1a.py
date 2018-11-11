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

# Function to remove "-8" or "-0" suffix from filenames, e.g. 12220-8.txt -> 12220.txt

def stripSuffix(f):
    x = re.sub(r'(\d+)(-\d+)(/|\.txt)', '\g<1>\g<3>', f)
    return x
    

# When I de-dupe files, I prefer to keep utf-8, then ascii (or other), then iso-latin1
# utf-8 is better than ascii because it preserves non-English characters
# iso-latin1 is worse than ascii, because these characters aren't parsed and split words

def encodingPrefs(f):
    if re.match(r'\d+-0\.txt', f):
        return 0
    elif re.match(r'\d+-8.txt', f):
        return 2
    else:
        return 1
        

# De-duping function that returns the first item per dupe-group, sorted by a provided key
# Used to de-dupe text file inputs

def dedupe(List, groupKey, sortKey, returnKey):
    List.sort(key=itemgetter(groupKey, sortKey))
    ret = []
    x = None
    for tup in List:
        if tup[groupKey] == x:
            continue
        else:
            ret += [tup[returnKey]]
            x = tup[groupKey]
    return ret


# This function helps me to emulate wholeTextFiles but keep extra flexibility of textFile

def addLines(x, y):
    return x + '\n' + y
        
        
# Regex patterns for identifying header, footer and ID
        
headSplit = re.compile(r'\n\*{3} ?START OF[^\*\n]+GUTENBERG[^\*\n]+\*{3}\n', \
    flags = re.IGNORECASE)
    
footSplit = re.compile(r'\*{3} ?(END OF[^\*\n]+GUTENBERG|START: FULL LICENSE)', \
    flags = re.IGNORECASE)
    
idMatcher = re.compile(r'\[ *(ebook|etext) *# *([0-9]+) *\]', flags = re.IGNORECASE)


# Turn regex match of numeric ID into integer if found, otherwise return -1 for not found

def intID(idMatcher, string, file):
    try:
        return int(re.search(idMatcher, string).group(2))
    except:
        return -1


# This custom non-word definition allows most common European non-English characters and
# hyphens and apostrophes, but excludes numbers (\W with unicode flag allows numbers).

nonW = re.compile(ur"[^A-Za-z\u00C0-\u01BF\'\-]+|\-{2,}")


# MAIN WORKHORSE FUNCTION FOR CREATING WORD-FREQUENCY PAIRS PER FILE

# First reduceByKey here appears to preserve line order - important for splitting header
# and footer. I assume there is an implicit sort created on importing via sc.textFile
# which is maintained until tuples are distributed on another key.

def countWords(sc, file):
    ret = sc.textFile(file) \
            .map(lambda x: (file, x)) \
            .reduceByKey(addLines) \
            .map(lambda (f, t): (intID(idMatcher, t, f), re.split(headSplit, t)[-1])) \
            .map(lambda (id, t): (id, re.split(footSplit, t)[0])) \
            .flatMap(lambda (id, t): [((id, w.lower()), 1) for w in re.split(nonW, t)]) \
            .filter(lambda (fw, c): fw[1] not in stopwords) \
            .reduceByKey(add)
    return ret
    

# Pickle an RDD, overwriting destination if it exists already

def pickle(rdd, destination):
    if os.path.exists(destination):
        shutil.rmtree(destination)
    rdd.saveAsPickleFile(destination)
    

# This creates a list of frequencies in order to calculate maximal term-frequency per file

def stripWords( (f, wcl) ):
    return (f, [c for (w, c) in wcl])

# --------------------------
# MAIN PROGRAM STARTS HERE
# --------------------------

if __name__ == "__main__":
    startJob = time()
    if len(sys.argv) != 8:
        print >> sys.stderr, "Usage: cwk.py <host> <library root>" \
        " <list of library child directories> <files per chunk> <max filesize in MiB>" \
        " <stopwords file> <start chunk>"
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
    
    
# This enables a complicated subset of directories to be chosen, useful for e.g. running
# libraries bigger than text-part but smaller than text-full   
    
    root = sys.argv[2] if sys.argv[2][-1] == "/" else sys.argv[2] + "/"
    dirs = sys.argv[3].split(',')
    
    
# The code from here on is wrapped in a try-finally block to ensure sc.stop() cleans up in
# the event of an exception or a ctrl-C termination
    
    try:
        sc = SparkContext(conf=config, appName="acka630")

        
# Generate all file-paths in parent directory(s) and de-dupe according to text-encoding
# preferences

        fps1 = []
        for dir in dirs:
            fps1 += [(r + '/' + f, f) for r, d, fl in os.walk(root + dir) for f in fl]
        fps2 = [(path, stripSuffix(f), encodingPrefs(f)) for (path, f) in fps1]
        filepaths = dedupe(fps2, 1, 2, 0)
    
        treeFileCount = len(filepaths)
        print "De-duped files (text and non-text) in tree:", treeFileCount
    

# Chunk the file-list up according to the specified number of files per chunk

        chunks = {}
        filesPerChunk = int(sys.argv[4])
        numChunks = int(ceil(float(treeFileCount)/filesPerChunk))
        
        for i in range(numChunks):
            chunks[i] = filepaths[i * filesPerChunk : (i+1) * filesPerChunk]
    

# Initialise dicts to store staging files

        chunkedWordCounts = {}
        chunkedFreqLists = {}
        chunkedNumFiles = {}
        chunkedDF = {}
        chunkedTF = {}


# Set initial file count to zero. Initialise an RDD to store word document frequencies

        numFiles = 0
        DF = sc.parallelize([], 16)


# User-supplied arguments. All files above maxSize MiB will be skipped, as will stopwords

        maxSize = float(sys.argv[5])
        stopwords = sc.textFile(sys.argv[6]).flatMap(lambda x: x.split(',')).collect()
        
        
# This block enables re-starting an aborted or crashed job from where it was left off
# To start a fresh job, specify startChunk = 0
        
        startChunk = int(sys.argv[7])
        
        if startChunk > 0:
            startFile = startChunk * filesPerChunk
            
    # Count how many files have already been processed (required for IDF calculation)
            for file in filepaths[:startFile]:
                if file[-4:] == '.txt' and os.path.getsize(file) <= (maxSize * 2**20):
                    numFiles += 1
            print numFiles, "files already processed"
            
            
    # Code further down saves checkpoints to this location. This re-opens the checkpoint
    # and converts log(IDF) values back to document frequencies so that new counts can be
    # added.
    
            DF = sc.pickleFile('pickles/IDF-checkpoint') \
                   .map(lambda (w, idf): (w, exp(-idf) * numFiles))
            vocabLength = DF.count()
            print "Vocab length so far:", vocabLength


# ----------------------------------------
# START OF PART 1: CREATE IDF AND TF FILES
# ----------------------------------------

        nFiles = 0
        
# START OF PER-CHUNK LOOP
        
        for i in range(startChunk, numChunks):
            fileWordCounts = sc.parallelize([])
            for file in chunks[i]:
                if nFiles % 100 == 0:
                    print "now walking file:", nFiles
                    
                    
# These if statements skip non-text files and files larger than maxSize MiB
                
                if file[-4:] != '.txt':
                    nFiles += 1
                    continue
                if os.path.getsize(file) > (maxSize * 2**20): 
                    nFiles += 1
                    continue
                    
                    
# Calls the countWords function to generate a ((file, word), count) list for this chunk
# merging each file's list with the lists from previous files in the chunk

                else:
                    wc = countWords(sc, file)
                    fileWordCounts = fileWordCounts.union(wc)
                nFiles += 1


# Cache resultant RDD as it will be used twice.

            chunkedWordCounts[i] = fileWordCounts.reduceByKey(add).cache()


# Map ((file, word), count) -> (file, [(word, count)]) and reduce to create a single
# frequency list per file. Cache as this RDD will also be used twice.
    
            chunkedFreqLists[i] = chunkedWordCounts[i] \
                                  .map(lambda (idw, count): (idw[0], [(idw[1], count)])) \
                                  .reduceByKey(add, 16) \
                                  .cache()

            startCount = time()
            chunkedNumFiles[i] = chunkedFreqLists[i].count()
            print "Total number of unique file IDs:", chunkedNumFiles[i]
            elapsedCounting = time() - startCount
            print "Elapsed creating freqLists:", elapsedCounting
            
            
# Map ((file, word), count) -> (word, 1) and reduce to create document frequency RDD

            chunkedDF[i] = chunkedWordCounts[i] \
                           .map(lambda (idw, count): (idw[1], 1)) \
                           .reduceByKey(add, 16)


# Append this chunk's document frequency RDD to previous sum of document frequencies.
# Keep track of total files processed to date and un-persist RDD to save memory usage.

            DF = DF.union(chunkedDF[i])
            numFiles += chunkedNumFiles[i]
            chunkedWordCounts[i].unpersist()
            

# Checkpointing code. This periodically transforms the document frequencies collected to
# date into an IDF RDD, and pickles this to disk. Saves re-running a large job in the
# event of a crash.
            
            if (i + 1) % 5 == 0:
                start = time()
                IDF = DF.reduceByKey(add, 16) \
                        .map(lambda (w, c): (w, log(float(numFiles)/float(c))))
                vocabLength = IDF.count()
                print "Vocab length:", vocabLength
                pickle(IDF, 'pickles/IDF-checkpoint')
                elapsed = time() - start
                print "IDF checkpointed as pickle file:", elapsed


# Calculate maximum term frequency per file

            countLists = chunkedFreqLists[i].map(stripWords)
            fileMax = countLists.map(lambda (id, counts): (id, float(max(counts))))


# Normalise absolute term frequency to 0:1 range, where 1 represents maximal TF, and swap
# keys to give a list of files and TFs per term (to enable joining of IDF on same key).

            chunkedTF[i] = chunkedFreqLists[i] \
                           .join(fileMax) \
                           .map(lambda (id, (wcl, maxf)): \
                                       (id, [(w, c/maxf) for (w, c) in wcl])) \
                           .flatMap(lambda (id, wtfl): \
                                           [(w, [(id, tf)]) for (w, tf) in wtfl]) \
                           .reduceByKey(add, 16) \
                           .cache()


# Save chunked term frequency RDD to disk as pickle file, and un-persist RDDs to conserve
# memory.

            start = time()
            pickle(chunkedTF[i], 'pickles/TF/TF' + '{:0>3d}'.format(i))
            elapsed = time() - start
            print "Term frequency chunk saved as pickle file:", elapsed
        
            chunkedFreqLists[i].unpersist()
            chunkedTF[i].unpersist()

# END OF PER-CHUNK LOOP


# Once all document frequency lists have been appended, create IDF RDD. Cache to allow
# counting and pickling without recalculation.

        start = time()
        IDF = DF.reduceByKey(add, 16) \
                .map(lambda (w, c): (w, log(float(numFiles)/float(c)))) \
                .cache()

        vocabLength = IDF.count()
        pickle(IDF, 'pickles/IDF')
        
        
# Output some interesting information
        
        print "Vocab length:", vocabLength
        elapsed = time() - start
        print "Elapsed creating and caching IDF and counting vocab:", elapsed
        
        elapsed = time() - startJob
        print "\nTotal elapsed time for Stage 1a:", elapsed, "\n"


# End of try-finally block. This ensures that spark context closes cleanly (e.g. deletes
# all its tmp files) in the case of error or ctrl-C termination.

    finally:
        start = time()
        sc.stop()
        elapsed = time() - start
        print "Elapsed stopping SparkContext:", elapsed
        totalElapsedTime = time() - startJob
        print "\nOverall time elapsed:", totalElapsedTime