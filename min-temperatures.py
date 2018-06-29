from pyspark import SparkConf, SparkContext


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)


### Function for parsing data
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)


### Load file
lines = sc.textFile("1800.csv")

### Call function 
parsedLines = lines.map(parseLine)

### Filter out all the values except "TMIN"
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

### StationID + Temperature
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

### Find minimum Temperature for each StationID
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

### Spark function for collecting data
results = minTemps.collect();

### Print to console
for result in results:
    print(result[0] + "\t{0}C".format(result[1]))
