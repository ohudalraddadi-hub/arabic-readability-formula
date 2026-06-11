import os
import sys

os.environ["JAVA_HOME"] = r"C:\Users\ohuda\anaconda3\Library\lib\jvm"
os.environ["PATH"] = r"C:\Users\ohuda\anaconda3\Library\lib\jvm\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, regexp_replace

# إنشاء SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("Arabic_Word_Frequency") \
    .getOrCreate()

# بيانات تجريبية
data = [("مرحبا كيف حالك",), ("هذا نص عربي تجريبي",)]
df = spark.createDataFrame(data, ["text"])

# حساب تردد الكلمات
words = df.select(explode(split(lower(col("text")), "\\s+")).alias("word"))
words = words.withColumn("word", regexp_replace(col("word"), "[^\\u0600-\\u06FF]", ""))
word_counts = words.filter(col("word") != "").groupBy("word").count().orderBy(col("count").desc())

word_counts.show()
print("تم! ✅")