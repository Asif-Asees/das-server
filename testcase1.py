from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("count1") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# Read the text file into a DataFrame
text_df = spark.read.text("vechicle.txt")

# Split each line into words and count the occurrences of each word
word_counts = text_df.select(explode(split(text_df.value, " ")).alias("word")) \
                     .groupBy("word") \
                     .agg(count("*").alias("count"))

# Show the word counts without truncation
word_counts.show(n=100)

# Stop the SparkSession
spark.stop()
