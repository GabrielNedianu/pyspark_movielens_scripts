from pyspark.sql import SparkSession

# Argument parser (at that path you should have the following files:)
# [movies.csv, ratings.csv, tags.csv, links.csv]
parser = argparse.ArgumentParser(description="Analyze MovieLens Ratings")
parser.add_argument("--path", required=True, help="Path to the folder containing MovieLens files")
args = parser.parse_args()

# Define file paths
base_path = args.path
movies_file = os.path.join(base_path, "movies.csv")
ratings_file = os.path.join(base_path, "ratings.csv")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieLens - Analyze Ratings") \
    .getOrCreate()

# Load datasets
movies = spark.read.csv(movies_file, header=True, inferSchema=True)
ratings = spark.read.csv(ratings_file, header=True, inferSchema=True)

# Calculate average rating for each movie
avg_ratings = ratings.groupBy("movieId").avg("rating").withColumnRenamed("avg(rating)", "average_rating")

# Find the number of ratings per movie
ratings_count = ratings.groupBy("movieId").count().withColumnRenamed("count", "rating_count")

# Join movies with ratings data
movie_stats = movies.join(avg_ratings, "movieId").join(ratings_count, "movieId")

# Top 10 movies by average rating
top_movies = movie_stats.orderBy("average_rating", ascending=False).limit(10)
print("Top 10 Movies by Average Rating:")
top_movies.show()

# Top 10 movies by number of ratings
most_rated_movies = movie_stats.orderBy("rating_count", ascending=False).limit(10)
print("\nTop 10 Movies by Number of Ratings:")
most_rated_movies.show()

# Stop SparkSession
spark.stop()
