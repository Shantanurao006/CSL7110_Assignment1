from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lower, regexp_replace, col
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
import math

# -----------------------------------------
# Create Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Q11_TFIDF_Book_Similarity") \
    .getOrCreate()

# Disable INFO logs (VERY IMPORTANT FOR SCREENSHOT)
spark.sparkContext.setLogLevel("ERROR")

print("\n==============================")
print("     Q11 - TF-IDF SIMILARITY")
print("==============================\n")

# -----------------------------------------
# Load Dataset
# -----------------------------------------
print("Step 1: Loading Dataset...\n")

books = spark.read.text("file:///home/student/spark/books/gutenberg_dataset/*.txt") \
    .withColumn("file_name", input_file_name())

print("Total Rows Loaded:", books.count())
print("Total Files:", books.select("file_name").distinct().count())
print("\n")

# -----------------------------------------
# Clean Text
# -----------------------------------------
print("Step 2: Cleaning Text...\n")

cleaned = books.withColumn(
    "clean_text",
    lower(regexp_replace(col("value"), "[^a-zA-Z\\s]", ""))
)

# -----------------------------------------
# Tokenization
# -----------------------------------------
print("Step 3: Tokenization & Stopword Removal...\n")

tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
wordsData = tokenizer.transform(cleaned)

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filteredData = remover.transform(wordsData)

# -----------------------------------------
# TF-IDF
# -----------------------------------------
print("Step 4: Generating TF-IDF...\n")

hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
featurizedData = hashingTF.transform(filteredData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# Show Sample TF-IDF
print("========== Sample TF-IDF Vectors ==========\n")
rescaledData.select("file_name", "features").show(5, truncate=False)
print("\n")

# -----------------------------------------
# Aggregate Per Book
# -----------------------------------------
print("Step 5: Aggregating Per Book...\n")

book_vectors = rescaledData.groupBy("file_name") \
    .agg({"rawFeatures": "count"}) \
    .withColumnRenamed("count(rawFeatures)", "line_count")

book_vectors.show(truncate=False)
print("\n")

# -----------------------------------------
# Collect Unique Book Names
# -----------------------------------------
files = rescaledData.select("file_name").distinct().collect()
file_list = [f["file_name"] for f in files]

# -----------------------------------------
# Cosine Similarity Function
# -----------------------------------------
def cosine_similarity(v1, v2):
    dot = v1.dot(v2)
    norm1 = math.sqrt(v1.dot(v1))
    norm2 = math.sqrt(v2.dot(v2))
    return dot / (norm1 * norm2)

# -----------------------------------------
# Compute Cosine Similarity
# -----------------------------------------
print("========== Cosine Similarity Between Books ==========\n")

# Collect one vector per book
book_feature = rescaledData.groupBy("file_name") \
    .agg({"features": "first"}) \
    .collect()

book_dict = {row["file_name"]: row["first(features)"] for row in book_feature}

for i in range(len(file_list)):
    for j in range(i + 1, len(file_list)):
        sim = cosine_similarity(book_dict[file_list[i]], book_dict[file_list[j]])
        print(f"Similarity between:")
        print(file_list[i].split("/")[-1])
        print("AND")
        print(file_list[j].split("/")[-1])
        print(f"= {round(sim,4)}\n")

print("=========== END OF Q11 OUTPUT ===========\n")

spark.stop()
