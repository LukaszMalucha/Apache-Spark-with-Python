from pyspark import SparkConf, SparkContext


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)


### Function for parsing data
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


### Load file
lines = sc.textFile("fakefriends.csv")
### Call function 
rdd = lines.map(parseLine)

###                   ( 36, 280) => (36, (280,1))        (36, (280,1)), (36, (220,1)) => (36, (500,2))    
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

### (36, (500,2))  => (36, 250)    
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

### Spark function for collecting data
results = averagesByAge.collect()
results.sort()

### Print to console
for result in results:
    print(result)
