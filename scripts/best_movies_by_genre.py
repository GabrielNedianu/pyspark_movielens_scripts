from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, count, row_number, concat, lit
from pyspark.sql.window import Window
import argparse

def main():
    # Argument parser (at that path you should have the following files:)
    # [movies.csv, ratings.csv, tags.csv, links.csv]
    parser = argparse.ArgumentParser(description="Find Best Movies per Genre")
    parser.add_argument('--path', type=str, required=True, help="Path to the MovieLens data")
    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder.appName("BestMoviesByGenre").getOrCreate()

    # Load datasets
    movies_df = spark.read.csv(f"{args.path}/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv(f"{args.path}/ratings.csv", header=True, inferSchema=True)
    links_df = spark.read.csv(f"{args.path}/links.csv", header=True, inferSchema=True)

    # Extract IMDb links
    links_df = links_df.withColumn("imdb_link", concat(lit("https://www.imdb.com/title/tt"), col("imdbId").cast("string")))

    # Split genres into multiple rows
    movies_df = movies_df.withColumn("genre", explode(split(col("genres"), "\\|")))

    # Compute average rating per movie and count of ratings
    ratings_summary_df = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("num_ratings")
    )

    # Join movie metadata (title, genre) with ratings
    movies_ratings_df = movies_df.join(ratings_summary_df, on="movieId", how="inner") \
                                 .join(links_df.select("movieId", "imdb_link"), on="movieId", how="left")

    # Filter out movies with less than 50 ratings
    movies_ratings_df = movies_ratings_df.filter(col("num_ratings") >= 50)

    # Define window function to rank movies within each genre
    window_spec = Window.partitionBy("genre").orderBy(col("avg_rating").desc())

    # Select the top-rated movie per genre
    best_movies_df = movies_ratings_df.withColumn("rank", row_number().over(window_spec)) \
                                      .filter(col("rank") == 1) \
                                      .select("genre", "title", "avg_rating", "num_ratings", "imdb_link")

    # Show results
    best_movies_df.show(truncate=False)

    # Paths for saving (relative to hdfs)
    csv_output_path = "hdfs:///movielens/output_best_movies_by_genre.csv"
    txt_output_path = "hdfs:///movielens/output_best_movies_by_genre.txt"

    # Save to CSV (structured format)
    best_movies_df.write.mode("overwrite").csv(csv_output_path, header=True)
    print(f"Results saved as CSV in {csv_output_path}")

    # Save to TXT (human-readable)
    with open(txt_output_path, "w") as txt_file:
        for row in best_movies_df.collect():
            txt_file.write(f"{row['genre']}: {row['title']} (Avg Rating: {row['avg_rating']:.2f}, Votes: {row['num_ratings']}) - {row['imdb_link']}\n")

    print(f"Results saved as TXT in {txt_output_path}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
