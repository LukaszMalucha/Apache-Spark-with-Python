from pyspark import SparkConf, SparkContext                                    ## RDD creation
import collections                                                             ## sort final results


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)                                                 ## create RDD 

## Load data file. One line = one value in RDD
lines = sc.textFile("ml-100k/u.data")

## Mapper Function - Extract rating value
ratings = lines.map(lambda x: x.split()[2])

## Transform RDD into value conter - pair (rating, rating count)
result = ratings.countByValue()


## Sort iteration: 
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
