from pyspark.sql import SparkSession

# Euclid, tim uoc so chung lon nhat
def gcd(a, b):
  while b != 0:
    a, b = b, a % b
  return a

spark = SparkSession.builder.appName("TongDayPhanSo").getOrCreate()

rdd = spark.sparkContext.textFile("data_phanso.txt")

day_phan_so = rdd.map(lambda line: tuple(map(int, line.strip().split("/"))))

tu_mau = day_phan_so.collect()

# Tinh mau so chung
mau_so_chung = 1
for _, mau in tu_mau:
    mau_so_chung *= mau

# Tinh tong tu so
tong_tu_so = 0
for tu, mau in tu_mau:
    tong_tu_so += tu * (mau_so_chung // mau)

# Rut gon
# Rút gọn phân số bằng gcd tự cài đặt
ucln = gcd(tong_tu_so, mau_so_chung)
tong_tu_so //= ucln
mau_so_chung //= ucln

print(f"Tong day phan so: {tong_tu_so}/{mau_so_chung}")
