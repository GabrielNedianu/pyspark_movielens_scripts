# 🎬 MovieLens Analysis
Movielens analysis pyspark scripts project
This scripts can be run in a pyspark env (preferabbly on a docker, I recommend the big-data-europe updated with pyspark and python 3.9)

This repository contains scripts for analyzing the MovieLens dataset using **PySpark** and **Hadoop** inside a **Docker environment**. The analysis focuses on various insights including **top-rated movies, genre trends, user behavior, and correlations**.

---

## 📌 Prerequisites

Before running the scripts, ensure you have the following installed:

- **Docker** (for running Hadoop & PySpark)
- **Git** (for cloning the repository)
- **Jupyter Notebook** (for visualization)

---

## 📂 Running the PySpark Scripts

All scripts are located in the `scripts/` folder. 

### 🔹 Load Data
```bash
spark-submit --master local[*] scripts/load_data.py --path hdfs:///movielens/
```
This script loads the dataset into.

### 🔹 Find Best Movies by Genre
```bash
spark-submit --master local[*] scripts/best_movies_by_genre.py --path hdfs:///movielens/
```
Finds the **best-rated movie for each genre** and saves results in HDFS.

### 🔹 Generate Data for Charts
```bash
spark-submit --master local[*] scripts/generate_chart_data.py --path hdfs:///movielens/
```
Generates **various statistical analyses** (ratings, trends, correlations) and stores them in HDFS.

### 🔹 Copy results from HDFS to Local
```bash
docker cp <namenode-container-id>:/tmp/charts C:\Programming\Movie_Lens\
```
This moves generated CSVs to your local machine.

---

## 📊 Visualizing the Data

### 1️⃣ Move the CSV files into the correct local directory:
```bash
mv C:\Programming\Movie_Lens\charts C:\Programming\Movie_Lens\analysis\charts
```

### 2️⃣ Start Jupyter Notebook:
```bash
jupyter notebook
```

### 3️⃣ Open the analysis notebook:
```
http://localhost:8888/notebooks/Movie_Lens/analysis/analysis.ipynb
```

### 4️⃣ Run the notebook to generate charts.

---

## 🔥 Key Analyses & Charts

1. **Ratings Distribution** - Histogram of all ratings.
2. **Top 10 Movies by Rating** - Bar chart of highest-rated movies.
3. **Ratings Over Time** - How movie ratings evolved yearly.
4. **Most Popular Genres** - Which genres received the most reviews over time.
5. **User Rating Behavior** - Scatter plot showing rating activity.
6. **Movie Rating Variance** - Box plot of rating consistency.
7. **Correlation: Reviews & Ratings** - Scatter plot showing relationship.
8. **Most Reviewed Genres Per Year** - Stacked bar chart.
9. **Most Popular Tags Per Year** - Bar chart of frequent tags.
10. **Yearly Trends: Ratings & Reviews** - Dual line chart.

---

## 🛠 Useful Commands

### 📂 Check HDFS Files:
```bash
hdfs dfs -ls /movielens/
```

### 📂 View File Content:
```bash
hdfs dfs -cat /movielens/output_best_movies_by_genre.txt/part-00000
```

### 🚀 Run PySpark Shell:
```bash
pyspark
```

### 🔄 Restart Docker Containers:
```bash
docker restart namenode datanode resourcemanager nodemanager historyserver
```

---

## 📝 Notes

- The script paths use `hdfs:///movielens/` since the dataset is stored in HDFS.
- If a script fails, check if **HDFS is running** and **data exists in HDFS** (`hdfs dfs -ls /movielens/`).
- To add **more nodes for parallel processing**, update the Spark submit command to use a **cluster** instead of `local[*]`.

---
