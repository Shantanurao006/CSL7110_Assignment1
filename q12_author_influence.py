from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    regexp_extract,
    col,
    min as spark_min,
    count
)
from pyspark.sql.types import IntegerType

# ----------------------------------------
# Create Spark Session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("Q12_Author_Influence") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n==============================")
print("Q12 - AUTHOR INFLUENCE NETWORK")
print("==============================\n")

# ----------------------------------------
# STEP 1: Load Dataset (LOCAL FS)
# IMPORTANT: Use file://
# ----------------------------------------
books_df = spark.read.text(
    "file:///home/student/spark/books/gutenberg_dataset"
).withColumn("file_name", input_file_name())

print("Total Books Loaded:",
      books_df.select("file_name").distinct().count())

# ----------------------------------------
# STEP 2: Extract Author
# Pattern: by Jane Austen
# (First occurrence only)
# ----------------------------------------
author_df = books_df.withColumn(
    "author",
    regexp_extract(
        col("value"),
        r'(?i)\bby\s+([A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z]+)',
        1
    )
)

author_per_file = author_df.filter(col("author") != "") \
    .groupBy("file_name") \
    .agg(spark_min("author").alias("author"))

# ----------------------------------------
# STEP 3: Extract Publication Year
# First 4-digit year (1800–2025)
# ----------------------------------------
year_df = books_df.withColumn(
    "year",
    regexp_extract(
        col("value"),
        r'((18|19|20)\d{2})',
        1
    ).cast(IntegerType())
)

year_per_file = year_df.groupBy("file_name") \
    .agg(spark_min("year").alias("release_year"))

# ----------------------------------------
# STEP 4: Combine Metadata
# ----------------------------------------
metadata_df = author_per_file.join(
    year_per_file,
    "file_name"
).filter(col("release_year").isNotNull())

print("\nExtracted Author & Release Year:")
metadata_df.select("author", "release_year") \
    .distinct() \
    .show(truncate=False)

# ----------------------------------------
# STEP 5: Build Influence Network
# author1 influences author2 if:
# year1 < year2
# ----------------------------------------
a1 = metadata_df.select("author", "release_year").alias("a1")
a2 = metadata_df.select("author", "release_year").alias("a2")

influence_df = a1.join(
    a2,
    (col("a1.release_year") < col("a2.release_year")) &
    (col("a1.author") != col("a2.author"))
).select(
    col("a1.author").alias("author1"),
    col("a2.author").alias("author2")
)

print("\nTotal Influence Relationships:",
      influence_df.count())

print("\nInfluence Edges (author1 → author2):")
influence_df.show(truncate=False)

# ----------------------------------------
# STEP 6: Top 5 Out-Degree
# ----------------------------------------
print("\nTop 5 Authors by Out-Degree:")
influence_df.groupBy("author1") \
    .agg(count("*").alias("out_degree")) \
    .orderBy(col("out_degree").desc()) \
    .show(5)

# ----------------------------------------
# STEP 7: Top 5 In-Degree
# ----------------------------------------
print("\nTop 5 Authors by In-Degree:")
influence_df.groupBy("author2") \
    .agg(count("*").alias("in_degree")) \
    .orderBy(col("in_degree").desc()) \
    .show(5)

print("\n=========== END OF Q12 OUTPUT ===========")

spark.stop()
