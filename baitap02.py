from pyspark.sql import SparkSession, functions as F
import random

spark = SparkSession.builder.appName("PhuongSaiDoLechChuanDaySoThuc").getOrCreate()

numbers = [random.uniform(0, 100) for _ in range(10)]

print(f"Day so thuc: {numbers}")

df = spark.createDataFrame([(float(x),) for x in numbers], ["value"])

# Tinh phuong sai
phuong_sai = df.agg(F.variance("value")).collect()[0][0]
print(f"Phuong sai: {phuong_sai}")

# Tinh do lech chuan
do_lech_chuan = df.agg(F.stddev("value")).collect()[0][0]
print(f"Do lech chuan: {do_lech_chuan}")