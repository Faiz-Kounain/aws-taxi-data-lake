"""
Glue job: taxi-raw-to-clean   (Spark script, 3 x G.1X workers, timeout 30 min)

raw/yellow_trips  ->  quality check  ->  rejected/  (bad rows + reject_reason)
                                     ->  clean/     (good rows + business columns)
clean             ->  gold/ daily_zone_revenue, hourly_demand, payment_tips

Job parameters
  --BUCKET   s3://faiz-taxi-lake-2026   (WITH s3:// in this job)
  --MONTH    all  (or 01, 02 ... to re-run one month)
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["BUCKET", "MONTH"])   # read the job parameters
B, MONTH = args["BUCKET"], args["MONTH"]
print("Running for:", B, "month:", MONTH)

spark = GlueContext(SparkContext.getOrCreate()).spark_session
# overwrite only the month folders we write, keep the others
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 1. READ
trips = spark.read.parquet(f"{B}/raw/yellow_trips/")
if MONTH != "all":
    trips = trips.filter(F.col("month") == MONTH)   # process only one month
zones = spark.read.option("header", "true").csv(f"{B}/raw/taxi_zones/")

# 2. QUALITY CHECK: give every bad row a reason
trips = trips.withColumn("reject_reason",
    F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0), "no passengers")
     .when(F.col("trip_distance") <= 0, "zero distance")
     .when(F.col("total_amount") <= 0, "bad amount")
     .when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), "bad time")
     .when(F.year("tpep_pickup_datetime") != 2024, "wrong year"))

bad  = trips.filter(F.col("reject_reason").isNotNull())
good = trips.filter(F.col("reject_reason").isNull()).drop("reject_reason").dropDuplicates()

bad.write.mode("overwrite").partitionBy("month").parquet(f"{B}/rejected/yellow_trips/")

# 3. ENRICH: add business columns
pu = zones.select(F.col("LocationID").cast("bigint").alias("PULocationID"),
                  F.col("Borough").alias("pickup_borough"), F.col("Zone").alias("pickup_zone"))
clean = (good.join(F.broadcast(pu), "PULocationID", "left")      # zones is tiny -> broadcast
  .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
  .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
  .withColumn("weekday", F.date_format("tpep_pickup_datetime", "E"))
  .withColumn("trip_minutes", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
  # F.when guards: Spark ANSI mode fails the job on x / 0 (DIVIDE_BY_ZERO), so divide only when safe
  .withColumn("speed_mph", F.when(F.col("trip_minutes") > 0,
        F.round(F.col("trip_distance") / (F.col("trip_minutes") / 60), 1)))
  .withColumn("tip_pct", F.when(F.col("fare_amount") > 0,
        F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 1)))
  .withColumn("is_airport", F.col("Airport_fee") > 0)
  .withColumn("payment", F.when(F.col("payment_type") == 1, "card")
                          .when(F.col("payment_type") == 2, "cash").otherwise("other")))

# 4. SAVE CLEAN (silver)
# repartition by the partition columns -> 1 file per month (before this fix: 48 small files)
clean.repartition("year", "month").write.mode("overwrite") \
     .partitionBy("year", "month").parquet(f"{B}/clean/yellow_trips/")
clean = spark.read.parquet(f"{B}/clean/yellow_trips/")   # read back = faster summaries

# 5. GOLD summaries for the dashboard
(clean.groupBy("pickup_date", "pickup_borough", "pickup_zone")
   .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"))
   .write.mode("overwrite").parquet(f"{B}/gold/daily_zone_revenue/"))

(clean.groupBy("weekday", "pickup_hour", "is_airport")
   .agg(F.count("*").alias("trips"), F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"))
   .write.mode("overwrite").parquet(f"{B}/gold/hourly_demand/"))

(clean.groupBy("month", "payment")
   .agg(F.count("*").alias("trips"), F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"))
   .write.mode("overwrite").parquet(f"{B}/gold/payment_tips/"))

print("raw:", trips.count(), "| rejected:", bad.count(), "| clean:", clean.count())
