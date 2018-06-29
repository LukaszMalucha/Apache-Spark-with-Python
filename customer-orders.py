from pyspark import SparkConf, SparkContext                                    ## RDD creation


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("CustomerOrder")
sc = SparkContext(conf = conf)                                                 ## create RDD 



### Function for parsing data
def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

## Load data file. One line = one value in RDD
lines = sc.textFile("customer-orders.csv")

### Call function 
rdd = lines.map(parseLine)

  
totalsByC = rdd.reduceByKey(lambda x, y: x+ y)


### Spark function for collecting data
results = totalsByC.collect()
results.sort()

### Print to console
for result in results:
    print(result)