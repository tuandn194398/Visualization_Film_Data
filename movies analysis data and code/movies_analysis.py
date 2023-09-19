import matplotlib
import matplotlib.pyplot as plt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

sc = SparkContext()

spark = SparkSession \
    .builder \
    .appName("Film rating analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

tagsFile = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://photon:9000/tags.csv")
moviesFile = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://photon:9000/movies.csv")
ratingsFile = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://photon:9000/ratings.csv")
movie100kFile = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://photon:9000/movie100k.csv")


tagsFile.createTempView("tags")
moviesFile.createTempView("movies")
ratingsFile.createTempView("ratings")
movie100kFile.createTempView("movies100k")


moviesFile.printSchema()
moviesFile.show()
movie100kFile.printSchema()
movie100kFile.show()


cleanMovies = spark.sql("SELECT movies.movieID, movies.title, movies100k.date "
                        "FROM movies "
                        "LEFT JOIN movies100k "
                        "ON movies.movieID=movies100k.movieID ")
spark.catalog.dropTempView("movies")
cleanMovies.createTempView("movies")


moviesRatings = spark.sql("SELECT movies.movieID, movies.title, ratings.userID, ratings.rating, ratings.timestamp "
                          "FROM movies "
                          "INNER JOIN ratings "
                          "ON movies.movieID=ratings.movieID "
                          "ORDER BY movieID, userID")
moviesRatings.createTempView("movies_rating")


data1Output = spark.sql("SELECT movieID, title, AVG(rating) AS average_rating "
                        "FROM movies_rating "
                        "GROUP BY movieID, title "
                        "ORDER BY movieID ")

data1Output.createTempView("avg_movie_rating")


movieTags = spark.sql("SELECT movies.movieID, movies.title, tags.tag "
                      "FROM movies "
                      "INNER JOIN tags "
                      "ON movies.movieID=tags.movieID "
                      "ORDER BY movieID ")

movieTags.createTempView("temp_movie_tags")
movieTags = spark.sql("SELECT DISTINCT *"
                      "FROM temp_movie_tags "
                      "ORDER BY tag")


movieTags.createTempView("movie_tags")

data2Output = spark.sql("SELECT tag, COUNT(movieID) AS numberOfMovies "
                        "FROM movie_tags "
                        "GROUP BY tag "
                        "ORDER BY numberOfMovies DESC ")

movieTagRatings = spark.sql("SELECT movie_tags.tag, movie_tags.title, avg_movie_rating.average_rating "
                            "FROM movie_tags "
                            "INNER JOIN avg_movie_rating "
                            "ON movie_tags.movieID=avg_movie_rating.movieID "
                            "ORDER BY tag ")
movieTagRatings.createTempView("movie_tag_rating")


data3Output = spark.sql("SELECT tag, AVG(average_rating) AS average_tag_rating "
                        "FROM movie_tag_rating "
                        "GROUP BY tag "
                        "ORDER BY tag ")
data3Output.createTempView('data3_out_put')


data1Output.write.option("header", "true").csv("hdfs://photon:9000/data1Output")
data2Output.write.option("header", "true").csv("hdfs://photon:9000/data2Output")
data3Output.write.option("header", "true").csv("hdfs://photon:9000/data3Output")

filter1 = data1Output.filter("average_rating>4").orderBy(desc("average_rating"))
filter1.write.option("header", "true").csv("hdfs://photon:9000/filter1Output")

pdf1 = data1Output.toPandas()
pdf2 = data2Output.toPandas()
data3Output = spark.sql("SELECT * "
                        "FROM data3_out_put "
                        "ORDER BY average_tag_rating DESC")
pdf3 = data3Output.toPandas()
print(pdf1)
print(pdf2)
print(pdf3)
pdf2 = pdf2.head(10)
x = list(pdf2.iloc[:, 0])
y = list(pdf2.iloc[:, 1])

plt.bar(x, y, color='g')
plt.xlabel("tag")
plt.ylabel("number of film")
plt.show()


plt.hist(pdf1['average_rating'], bins=100)
plt.xlabel('rating')
plt.ylabel('Number of movies')
plt.title('Histogram of rating')
plt.show()


pdf3 = pdf3.head(20)
x = list(pdf3.iloc[:, 0])
y = list(pdf3.iloc[:, 1])
plt.xticks(rotation=30, ha='right')
plt.bar(x, y, color='red')
plt.xlabel("Tag")
plt.ylabel("Average rating")
plt.title("Most rating tag")
plt.show()




