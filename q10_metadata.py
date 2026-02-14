from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, length, avg, col

# Create Spark Session
spark = SparkSession.builder.appName("Q10_Metadata_Extraction").getOrCreate()

# Read all Gutenberg files (LOCAL FILES)
books_df = spark.read.text("file:///home/student/spark/books/gutenberg_dataset/*.txt")

# Add file name column
books_df = books_df.withColumn("file_name", input_file_name())

# Extract Title
books_df = books_df.withColumn(
    "title",
    regexp_extract("value", r"Title:\s*(.*)", 1)
)

# Extract Release Date
books_df = books_df.withColumn(
    "release_date",
    regexp_extract("value", r"Release Date:\s*(.*)", 1)
)

# Extract Language
books_df = books_df.withColumn(
    "language",
    regexp_extract("value", r"Language:\s*(.*)", 1)
)

# Extract Encoding
books_df = books_df.withColumn(
    "encoding",
    regexp_extract("value", r"Character set encoding:\s*(.*)", 1)
)

print("\n========== Extracted Metadata ==========\n")
books_df.filter(col("title") != "").show(truncate=False)

# Count books per year
books_per_year = books_df.filter(col("release_date") != "") \
    .withColumn("year", regexp_extract("release_date", r"(\d{4})", 1)) \
    .groupBy("year").count()

print("\n========== Books Released Per Year ==========\n")
books_per_year.show()

# Most common language
most_common_language = books_df.filter(col("language") != "") \
    .groupBy("language").count() \
    .orderBy(col("count").desc())

print("\n========== Most Common Language ==========\n")
most_common_language.show(1)

# Average title length
avg_title_length = books_df.filter(col("title") != "") \
    .withColumn("title_length", length("title")) \
    .select(avg("title_length"))

print("\n========== Average Title Length ==========\n")
avg_title_length.show()

spark.stop()
