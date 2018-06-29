import re
from pyspark import SparkConf, SparkContext


### Strip out punctuation and lowerize words
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

### Load file
input = sc.textFile("file:///sparkcourse/book.txt")

### Use flatMap instead of map so every line will be split on each word value
words = input.flatMap(normalizeWords)


###                    word => ('word',1)            add all occurences   
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

### Swap (word,count) into (count,word) and sort by Key(count)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

### Spark function for collecting data
results = wordCountsSorted.collect()


### Converting into ascii for terminal display
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
