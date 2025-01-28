from pyspark.sql import SparkSession

# Argument parser (at that path you should have the following files:)
# [movies.csv, ratings.csv, tags.csv, links.csv]
parser = argparse.ArgumentParser(description="Load MovieLens Dataset")
parser.add_argument("--path", required=True, help="Path to the folder containing MovieLens files")
args = parser.parse_args()

# Define file paths
base_path = args.path
movies_file = os.path.join(base_path, "movies.csv")
ratings_file = os.path.join(base_path, "ratings.csv")
tags_file = os.path.join(base_path, "tags.csv")
links_file = os.path.join(base_path, "links.csv")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieLens - Load Data") \
    .getOrCreate()

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
