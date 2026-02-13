# CSL7110 Assignment 1
## Hadoop MapReduce – WordCount Implementation

---

## 👨‍🎓 Student Details
Name: Shantanu Rao  
Roll Number: <M24DE2026>

---

## 📌 Objective
Install Hadoop locally and implement the WordCount program using MapReduce framework.

---

## 🛠 Technologies Used
- Hadoop 3.3.6
- Java 8
- Ubuntu (WSL)
- HDFS
- Git & GitHub

---

## 📂 Project Structure

CSL7110/
│
├── src/
│   └── WordCount.java
├── classes/
├── WordCount.jar
├── 200.txt
└── README.md

---

## 🚀 Steps to Run the Program

### 1️⃣ Start Hadoop

start-dfs.sh

Check:

jps

Expected:
NameNode
DataNode
SecondaryNameNode

---

### 2️⃣ Upload file to HDFS

hdfs dfs -mkdir -p /user/student
hdfs dfs -put 200.txt /user/student/

Check:

hdfs dfs -ls /user/student

---

### 3️⃣ Run WordCount

hadoop jar WordCount.jar WordCount /user/student/200.txt output

---

### 4️⃣ View Output

hdfs dfs -cat output/part-r-00000

---

## ⏱ Execution Time Measurement

Run with timing:

time hadoop jar WordCount.jar WordCount /user/student/200.txt output

Note the real time displayed.

---

## 📊 Performance Experiment

We tested different split sizes:

Default run:
time hadoop jar WordCount.jar WordCount /user/student/200.txt output1

Run with custom split size:

hadoop jar WordCount.jar WordCount \
-D mapreduce.input.fileinputformat.split.maxsize=1048576 \
/user/student/200.txt output2

Compare execution times.

Observation:
Smaller split size increases number of mappers and may affect performance.

---

## 🧠 Understanding Mapper and Reducer

### Mapper
- Reads input line
- Cleans text
- Emits (word, 1)

### Reducer
- Receives grouped words
- Adds all counts
- Outputs (word, total_count)

---

