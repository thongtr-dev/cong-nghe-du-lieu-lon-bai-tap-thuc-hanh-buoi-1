from pyspark.sql import SparkSession, functions as F
import math
import random

spark = SparkSession.builder.appName("TrungBinhCongTrungBinhNhanDaySoThuc").getOrCreate()

numbers = [random.uniform(0, 100) for _ in range(10)]

print(f"Day so thuc: {numbers}")

df = spark.createDataFrame([(float(x),) for x in numbers], ["value"])

# Tinh trung binh cong
trung_binh_cong = df.agg(F.avg("value")).collect()[0][0]

# Tinh trung binh nhan
df_positive = df.filter(df.value > 0)
if (df_positive.count() == len(numbers)):
    log_mean = df_positive.agg(F.avg(F.log("value"))).collect()[0][0]
    trung_binh_nhan = math.exp(log_mean)
else:
    trung_binh_nhan = None

print(f"Trung binh cong: {trung_binh_cong}")
print(f"Trung binh nhan: {trung_binh_nhan}")