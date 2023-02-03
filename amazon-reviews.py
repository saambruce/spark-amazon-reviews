#This program assumes that the input file has a header row 
#with header title of "star_rating" and the header title
#will never be repeated in the entire input file
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: average.py <input> <output>"
        exit(-1)
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.map(lambda line: line.split('\t')) \
        .filter(lambda filt_line: len(filt_line) == 15) \
        .filter(lambda filt: "star_rating" not in filt) \
        .persist()
    rdd_count = rdd.map(lambda x: (x[3],1)) \
        .reduceByKey(lambda x, y: x+y) \
        .sortByKey()
    
    rdd_average = rdd.map(lambda fields: (fields[3], float(fields[7]))) \
        .groupByKey().mapValues(lambda x : sum(x)/len(x)) \
        .sortByKey() 
    final_rdd = rdd_average.join(rdd_count) \
        .sortByKey()
    
    final_rdd.saveAsTextFile(sys.argv[2])
    
    sc.stop()
