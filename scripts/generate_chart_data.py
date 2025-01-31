from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, stddev, year, explode, split, desc, row_number, lit, from_unixtime
from pyspark.sql.window import Window
import argparse

def main():
    # Argument parser for dataset path
    parser = argparse.ArgumentParser(description="Generate CSV Data for Chart Analysis")
    parser.add_argument('--path', type=str, required=True, help="Path to the MovieLens dataset")
    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder.appName("ChartDataGeneration").getOrCreate()

    # Load datasets
    movies_df = spark.read.csv(f"{args.path}/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv(f"{args.path}/ratings.csv", header=True, inferSchema=True)
    tags_df = spark.read.csv(f"{args.path}/tags.csv", header=True, inferSchema=True)

    # Create output folder in HDFS
    output_path = "hdfs:///movielens/charts/"

    # 1️⃣ **Ratings Distribution** (Histogram)
    ratings_distribution = ratings_df.groupBy("rating").count().orderBy("rating")
    ratings_distribution.write.mode("overwrite").csv(output_path + "ratings_distribution.csv", header=True)

    # 2️⃣ **Top Movies by Average Rating** (Bar Chart)
    top_movies = ratings_df.groupBy("movieId").agg(avg("rating").alias("avg_rating"), count("rating").alias("num_ratings"))
    top_movies = top_movies.filter(col("num_ratings") >= 50).orderBy(desc("avg_rating")).limit(50)
    top_movies = top_movies.join(movies_df, "movieId").select("title", "avg_rating", "num_ratings")
    top_movies.write.mode("overwrite").csv(output_path + "top_movies_by_rating.csv", header=True)

    # 3️⃣ **Top Rated Movies Over Time** (Line Chart)
    ratings_over_time = ratings_df.withColumn("year", year(from_unixtime(col("timestamp")))) \
                                  .groupBy("year", "movieId") \
                                  .agg(avg("rating").alias("avg_rating"), count("rating").alias("num_ratings"))
    ratings_over_time = ratings_over_time.join(movies_df, "movieId") \
                                         .select("year", "title", "avg_rating", "num_ratings")
    ratings_over_time.write.mode("overwrite").csv(output_path + "top_movies_over_time.csv", header=True)

    # 4️⃣ **Most Popular Genres Over Time** (Stacked Bar Chart)
    movies_genres_df = movies_df.withColumn("genre", explode(split(col("genres"), "\\|")))
    popular_genres = ratings_df.join(movies_genres_df, "movieId") \
                               .withColumn("year", year(from_unixtime(col("timestamp")))) \
                               .groupBy("year", "genre") \
                               .agg(count("rating").alias("num_ratings"))
    popular_genres.write.mode("overwrite").csv(output_path + "popular_genres_over_time.csv", header=True)

    # 5️⃣ **User Rating Behavior** (Heatmap)
    user_behavior = ratings_df.groupBy("userId").agg(count("rating").alias("num_ratings"), avg("rating").alias("avg_rating"))
    user_behavior.write.mode("overwrite").csv(output_path + "user_rating_behavior.csv", header=True)

    # 6️⃣ **Movie Rating Variance** (Box Plot)
    movie_variance = ratings_df.groupBy("movieId").agg(stddev("rating").alias("rating_stddev"), count("rating").alias("num_ratings"))
    movie_variance = movie_variance.filter(col("num_ratings") >= 50).join(movies_df, "movieId").select("title", "rating_stddev", "num_ratings")
    movie_variance.write.mode("overwrite").csv(output_path + "movie_rating_variance.csv", header=True)

    # 7️⃣ **Correlation Between Number of Ratings & Average Score** (Scatter Plot)
    correlation_data = top_movies.select("num_ratings", "avg_rating")
    correlation_data.write.mode("overwrite").csv(output_path + "ratings_correlation.csv", header=True)

    # 8️⃣ **Most Popular Genres Per Year** (Stacked Bar Chart)
    most_reviewed_genres = ratings_df.join(movies_genres_df, "movieId") \
                                     .withColumn("year", year(from_unixtime(col("timestamp")))) \
                                     .groupBy("year", "genre") \
                                     .agg(count("rating").alias("num_reviews"), avg("rating").alias("avg_rating")) \
                                     .orderBy("year", desc("num_reviews"))
    most_reviewed_genres.write.mode("overwrite").csv(output_path + "most_popular_genres_per_year.csv", header=True)

    # 9️⃣ **Most Popular Tags Per Year** (Bar Chart)
    popular_tags = tags_df.withColumn("year", year(from_unixtime(col("timestamp")))) \
                          .groupBy("year", "tag") \
                          .agg(count("movieId").alias("num_reviews")) \
                          .orderBy("year", desc("num_reviews"))
    popular_tags.write.mode("overwrite").csv(output_path + "most_popular_tags_per_year.csv", header=True)

    # 🔟 **Yearly Trends of Average Rating & Review Volume** (Line Chart)
    yearly_trends = ratings_df.withColumn("year", year(from_unixtime(col("timestamp")))) \
                              .groupBy("year") \
                              .agg(count("rating").alias("total_reviews"), avg("rating").alias("avg_rating"))
    yearly_trends.write.mode("overwrite").csv(output_path + "yearly_rating_trends.csv", header=True)

    print("✅ All 10 chart datasets have been saved in HDFS under /movielens/charts/")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
