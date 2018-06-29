from pyspark import SparkConf, SparkContext



### Create dictionray that maps movieID with movie name
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


### Create Spark Context. Master Node - local machine, set App name
conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

## Broadcast movienames into Spark context
nameDict = sc.broadcast(loadMovieNames())

## Load data file. One line = one value in RDD
lines = sc.textFile("ml-100k/u.data")

##                              ('movieID', 1)                        
movies = lines.map(lambda x: (int(x.split()[1]), 1))

## Count how man times each movie appeared
movieCounts = movies.reduceByKey(lambda x, y: x + y)

## Swap so (values, movieID)
flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

## Replace movieID with name
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))


results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
