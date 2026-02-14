CSL7110 Assignment 1
Apache Spark – Text Analytics & Author Network Implementation

👨‍🎓 Student Details
Name: Shantanu Rao
Roll Number: D24DE2026

📌 Objective
Install Apache Spark locally and perform large-scale text processing on Project Gutenberg dataset using PySpark. The implementation includes metadata extraction, TF-IDF computation, cosine similarity calculation, and author influence network construction.

🛠 Technologies Used
Apache Spark 3.5.2
Python (PySpark)
Java 8
Ubuntu (WSL)
Project Gutenberg Dataset
Git & GitHub

📂 Project Structure
spark/
│
├── q10_metadata.py
├── q11_tfidf_similarity.py
├── q12_author_influence.py
├── books/
│   └── gutenberg_dataset/
└── README.md


🚀 Steps to Run the Programs

1️⃣ Start Spark (if required)

Navigate to Spark directory:

cd ~/spark

Verify Spark installation:

spark-submit --version


2️⃣ Run Metadata Extraction Program

spark-submit q10_metadata.py

This program:
- Extracts title, release date, language, encoding
- Calculates books released per year
- Finds most common language
- Computes average title length


3️⃣ Run TF-IDF and Book Similarity Program

spark-submit q11_tfidf_similarity.py

This program:
- Cleans and preprocesses text
- Tokenizes and removes stopwords
- Computes TF (Term Frequency)
- Computes IDF (Inverse Document Frequency)
- Generates TF-IDF vectors
- Calculates cosine similarity between books
- Identifies most similar books


4️⃣ Run Author Influence Network Program

spark-submit q12_author_influence.py

This program:
- Extracts author names
- Extracts release year
- Constructs influence relationships
- Computes in-degree and out-degree
- Identifies top influential authors


📊 Understanding TF-IDF

TF (Term Frequency)
Measures how often a word appears in a document.

IDF (Inverse Document Frequency)
Measures how rare a word is across all documents.

TF-IDF Score
TF × IDF
Highlights important words while reducing the impact of common words.

Cosine Similarity
Measures similarity between two books by calculating the cosine of the angle between their TF-IDF vectors.
Value closer to 1 means highly similar.


📈 Author Influence Network Logic

An author is considered to influence another author if:
- Their book was released earlier.
- The release year difference falls within a defined range.

Out-Degree:
Number of authors influenced by a given author.

In-Degree:
Number of authors influencing a given author.


⚙ Scalability Notes

Spark handles large datasets by:
- Distributing data across partitions
- Parallel processing
- Lazy evaluation
- Optimized memory management

This makes Spark suitable for handling millions of documents efficiently.


📌 Conclusion

This Spark implementation demonstrates distributed data processing for:
- Metadata extraction
- Text cleaning and transformation
- TF-IDF computation
- Document similarity measurement
- Basic graph-style network analysis

All programs were executed successfully using Ubuntu (WSL) on Windows.
