from pyspark import SparkConf, SparkContext


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)


### Name parser
def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

### Occurence counter
def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)


## Load names file and use parseNames
names = sc.textFile("marvel-names.txt")
namesRdd = names.map(parseNames)


## Load social network and use countCoOccurences
lines = sc.textFile("marvel-graph.txt")
pairings = lines.map(countCoOccurences)

## Add friend occurences in social network
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)

## Swap into (occurences, heroID)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))


## Most popular
mostPopular = flipped.max()

## Monst popular name lookup
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
