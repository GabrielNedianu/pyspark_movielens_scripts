from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieLens - Load Data") \
    .getOrCreate()

# Define file paths (you need also to have this data locally)
movies_file = "data/movies.csv"
ratings_file = "data/ratings.csv"
tags_file = "data/tags.csv"
links_file = "data/links.csv"

# Load datasets
movies = spark.read.csv(movies_file, header=True, inferSchema=True)
ratings = spark.read.csv(ratings_file, header=True, inferSchema=True)
tags = spark.read.csv(tags_file, header=True, inferSchema=True)
links = spark.read.csv(links_file, header=True, inferSchema=True)

# Show sample data
print("Movies Dataset:")
movies.show(5)
movies.printSchema()

print("\nRatings Dataset:")
ratings.show(5)
ratings.printSchema()

print("\nTags Dataset:")
tags.show(5)
tags.printSchema()

print("\nLinks Dataset:")
links.show(5)
links.printSchema()

# Stop SparkSession
spark.stop()
